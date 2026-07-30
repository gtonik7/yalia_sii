import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { InjectDataSource, InjectRepository } from '@nestjs/typeorm';
import { DataSource, Repository } from 'typeorm';
import { RETENTION_CATALOG, RetentionTargetDef, findRetentionTarget } from './retention-catalog';
import { RetentionSetting } from './entities/retention-setting.entity';

const DAY_MS = 86_400_000;
const BATCH_SIZE = Number(process.env.RETENTION_BATCH_SIZE) || 5000;

export interface RetentionTargetView {
  key: string;
  label: string;
  description: string;
  readonly: boolean;
  retentionDays: number;
  intervalDays: number;
  enabled: boolean;
  sizeBytes: number | null;
  sizePretty: string | null;
  lastRunAt: string | null;
  nextRunAt: string | null;
  daysUntilNextPurge: number | null;
}

export interface RetentionConfigPatch {
  retentionDays?: number;
  intervalDays?: number;
  enabled?: boolean;
}

/**
 * Config (hot, en `retention_settings`) + listado con cuenta atrás y tamaño de los
 * targets de retención del satélite, y la mecánica de purga por lotes / drop_chunks que
 * ejecuta el MaintenanceProcessor (operación `retention.purge`). Espejo de la del hub.
 */
@Injectable()
export class RetentionService {
  private readonly logger = new Logger(RetentionService.name);

  constructor(
    @InjectDataSource() private readonly ds: DataSource,
    @InjectRepository(RetentionSetting) private readonly settings: Repository<RetentionSetting>,
  ) {}

  async getConfig(def: RetentionTargetDef): Promise<{ retentionDays: number; intervalDays: number; enabled: boolean; lastRunAt: Date | null }> {
    const row = def.readonly ? null : await this.settings.findOne({ where: { targetKey: def.key } });
    return {
      retentionDays: row?.retentionDays ?? def.defaultRetentionDays,
      intervalDays: row?.intervalDays ?? def.defaultIntervalDays,
      enabled: row?.enabled ?? def.defaultEnabled,
      lastRunAt: row?.lastRunAt ?? null,
    };
  }

  async setConfig(key: string, patch: RetentionConfigPatch): Promise<void> {
    const def = findRetentionTarget(key);
    if (!def) throw new BadRequestException(`Unknown retention target '${key}'`);
    if (def.readonly) throw new BadRequestException(`El target '${key}' es persistente y no se configura aquí`);

    const current = await this.getConfig(def);
    const next: Partial<RetentionSetting> = {
      targetKey: key,
      retentionDays: current.retentionDays,
      intervalDays: current.intervalDays,
      enabled: current.enabled,
    };

    if (patch.retentionDays !== undefined) {
      const n = Number(patch.retentionDays);
      if (!Number.isInteger(n) || n < def.minRetentionDays) {
        throw new BadRequestException(`retentionDays debe ser un entero ≥ ${def.minRetentionDays}`);
      }
      next.retentionDays = n;
    }
    if (patch.intervalDays !== undefined) {
      const n = Number(patch.intervalDays);
      if (!Number.isInteger(n) || n < 1) throw new BadRequestException('intervalDays debe ser un entero ≥ 1');
      next.intervalDays = n;
    }
    if (patch.enabled !== undefined) {
      if (typeof patch.enabled !== 'boolean') throw new BadRequestException('enabled debe ser booleano');
      next.enabled = patch.enabled;
    }
    await this.settings.upsert(next as RetentionSetting, ['targetKey']);
  }

  async listTargets(): Promise<RetentionTargetView[]> {
    return Promise.all(RETENTION_CATALOG.map((def) => this.buildView(def)));
  }

  private async buildView(def: RetentionTargetDef): Promise<RetentionTargetView> {
    const cfg = await this.getConfig(def);
    const size = await this.tableSize(def.sizeTable);
    const lastRunAt = cfg.lastRunAt;
    const nextRunMs = lastRunAt ? lastRunAt.getTime() + cfg.intervalDays * DAY_MS : Date.now();
    const daysUntilNextPurge = Math.max(0, Math.ceil((nextRunMs - Date.now()) / DAY_MS));

    return {
      key: def.key,
      label: def.label,
      description: def.description,
      readonly: def.readonly ?? false,
      retentionDays: cfg.retentionDays,
      intervalDays: cfg.intervalDays,
      enabled: cfg.enabled,
      sizeBytes: size?.bytes ?? null,
      sizePretty: size?.pretty ?? null,
      lastRunAt: lastRunAt ? lastRunAt.toISOString() : null,
      nextRunAt: !def.readonly && cfg.enabled ? new Date(nextRunMs).toISOString() : null,
      daysUntilNextPurge: !def.readonly && cfg.enabled ? daysUntilNextPurge : null,
    };
  }

  private async tableSize(table: string): Promise<{ bytes: number; pretty: string } | null> {
    try {
      const [row] = await this.ds.query(
        `SELECT pg_total_relation_size($1::regclass) AS bytes, pg_size_pretty(pg_total_relation_size($1::regclass)) AS pretty`,
        [table],
      );
      return { bytes: Number(row.bytes), pretty: row.pretty };
    } catch {
      return null;
    }
  }

  /**
   * Ejecuta la purga de un target (llamado por el MaintenanceProcessor). Borra por lotes
   * (o drop_chunks para hypertables) actualizando el progreso, marca `last_run_at` y
   * devuelve cuántas filas se borraron. `days` se valida como entero antes de interpolarse
   * en el INTERVAL; table/dateColumn provienen del catálogo estático → seguros.
   */
  async executePurge(targetKey: string, onProgress: (processed: number, total: number) => Promise<void>): Promise<number> {
    const def = findRetentionTarget(targetKey);
    if (!def) throw new Error(`Unknown retention target '${targetKey}'`);
    if (def.readonly || !def.executor) throw new BadRequestException(`El target '${targetKey}' no es purgable`);

    const cfg = await this.getConfig(def);
    const days = Math.floor(cfg.retentionDays);
    if (!Number.isFinite(days) || days < 0) throw new Error(`Invalid retention window: ${cfg.retentionDays}`);
    const cutoff = `now() - INTERVAL '${days} days'`;

    let deleted = 0;
    if (def.executor.kind === 'drop_chunks') {
      const { hypertable, dateColumn } = def.executor;
      const [{ total }] = await this.ds.query(`SELECT count(*)::int AS total FROM ${hypertable} WHERE ${dateColumn} < ${cutoff}`);
      await onProgress(0, total);
      try {
        await this.ds.query(`SELECT drop_chunks('${hypertable}', older_than => INTERVAL '${days} days')`);
        deleted = total;
      } catch {
        // Sin Timescale (dev): DELETE por fecha en lotes como fallback.
        deleted = await this.batchedDelete(hypertable, `${dateColumn} < ${cutoff}`, total, onProgress);
      }
      await onProgress(deleted, Math.max(total, deleted));
    } else {
      const { table, dateColumn, extraWhere } = def.executor;
      const where = `${dateColumn} < ${cutoff}${extraWhere ? ` AND ${extraWhere}` : ''}`;
      const [{ total }] = await this.ds.query(`SELECT count(*)::int AS total FROM ${table} WHERE ${where}`);
      await onProgress(0, total);
      deleted = total === 0 ? 0 : await this.batchedDelete(table, where, total, onProgress);
    }

    await this.settings.upsert(
      { targetKey, retentionDays: cfg.retentionDays, intervalDays: cfg.intervalDays, enabled: cfg.enabled, lastRunAt: new Date() },
      ['targetKey'],
    );
    this.logger.log(`Retention purge target=${targetKey} days=${days} deleted=${deleted}`);
    return deleted;
  }

  private async batchedDelete(
    table: string,
    where: string,
    total: number,
    onProgress: (processed: number, total: number) => Promise<void>,
  ): Promise<number> {
    let processed = 0;
    for (;;) {
      const [{ n }] = await this.ds.query(
        `WITH del AS (
           DELETE FROM ${table}
           WHERE ctid IN (SELECT ctid FROM ${table} WHERE ${where} LIMIT ${BATCH_SIZE})
           RETURNING 1
         ) SELECT count(*)::int AS n FROM del`,
      );
      processed += n;
      await onProgress(processed, Math.max(total, processed));
      if (n < BATCH_SIZE) break;
    }
    return processed;
  }
}
