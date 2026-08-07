import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';

/** Tras cuánto tiempo sin sellar se considera una carga colgada (lote perdido). */
const DEFAULT_STALE_LOAD_TIMEOUT_MS = 20 * 60_000;

/**
 * Red de seguridad del sellado de cargas (ver `TableLoadStaging` /
 * `TableRowsService.sealLoad`). Una carga fragmentada se sella cuando llegan sus
 * N lotes; si uno se pierde de forma permanente (su saga acabó en la DLQ), la
 * carga nunca completa y sus filas quedan `staged` para siempre.
 *
 * Este reaper NO auto-envía la carga a medias (rompería la invariante de "nunca
 * un lote parcial"): solo la marca (`stale_at`) y la registra para que un
 * operador la vea. El lote que falta es reejecutable desde la DLQ; al reintentarlo
 * la carga completa y se sella con normalidad (dejando `stale_at` como marca
 * histórica). Es idempotente: solo marca cargas sin sellar y aún no marcadas.
 */
@Injectable()
export class StaleLoadCron {
  private readonly logger = new Logger(StaleLoadCron.name);
  private readonly timeoutMs = Number(process.env.STALE_LOAD_TIMEOUT_MS) || DEFAULT_STALE_LOAD_TIMEOUT_MS;

  constructor(@InjectDataSource() private readonly dataSource: DataSource) {}

  @Cron(CronExpression.EVERY_MINUTE)
  async reap(): Promise<void> {
    try {
      const stale: { load_id: string; table_key: string; expected_batches: number | null; received: number }[] =
        await this.dataSource.query(
          `UPDATE table_loads
             SET stale_at = now()
           WHERE sealed_at IS NULL AND stale_at IS NULL
             AND created_at < now() - ($1::bigint * interval '1 millisecond')
           RETURNING load_id, table_key, expected_batches,
             (SELECT count(*)::int FROM table_load_batches b WHERE b.load_id = table_loads.load_id) AS received`,
          [this.timeoutMs],
        );
      for (const l of stale) {
        this.logger.warn(
          `Carga colgada sin sellar table=${l.table_key} load=${l.load_id} ` +
            `lotes=${l.received}/${l.expected_batches ?? '?'} — filas retenidas en 'staged' ` +
            `(reejecuta el lote que falta desde la DLQ para completarla; nunca se envía parcial)`,
        );
      }
    } catch (err) {
      this.logger.error(`Stale-load reaper failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
}
