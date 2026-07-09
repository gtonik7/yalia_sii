import { BadRequestException, Injectable, InternalServerErrorException, Logger } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { createHash } from 'node:crypto';
import { TableTemplate } from './entities/table-template.entity';
import { assertColumnKey, assertTableKey, sqlStringLiteral } from '../core/sql/sql-params.util';

type IndexSpec =
  | { kind: 'filter'; tableKey: string; columnKey: string }
  | { kind: 'unique-id'; tableKey: string; idField: string }
  | { kind: 'group'; tableKey: string; groupBy: string[] };

function shortHash(input: string): string {
  return createHash('sha256').update(input).digest('hex').slice(0, 16);
}

/**
 * Maintains per-template Postgres indexes on the shared `table_rows` hypertable
 * so `filterable`/`sortable` columns and `idField` upserts stay indexed without
 * ever giving a template its own physical columns (see the plan's storage
 * decision: one JSONB `data` column shared by every template).
 *
 * Index names are a deterministic hash of (tableKey, columnKey) — or
 * (tableKey, idField) for the unique index, which folds the *value* of
 * idField into the hash so renaming idField produces a different name (the
 * stale index gets dropped instead of silently no-op'ing against the wrong
 * expression via `IF NOT EXISTS`).
 */
@Injectable()
export class TableIndexManagerService {
  private readonly logger = new Logger(TableIndexManagerService.name);

  constructor(@InjectDataSource() private readonly dataSource: DataSource) {}

  async syncIndexes(oldTemplate: TableTemplate | null, newTemplate: TableTemplate): Promise<void> {
    const desired = this.desiredIndexes(newTemplate);
    const previous = oldTemplate ? this.desiredIndexes(oldTemplate) : new Map<string, IndexSpec>();
    for (const [name, spec] of desired) await this.ensureIndex(name, spec);
    for (const [name] of previous) if (!desired.has(name)) await this.dropIndex(name);
  }

  async dropAllIndexes(template: TableTemplate): Promise<void> {
    for (const [name] of this.desiredIndexes(template)) await this.dropIndex(name);
  }

  private desiredIndexes(tpl: TableTemplate): Map<string, IndexSpec> {
    const out = new Map<string, IndexSpec>();
    for (const col of tpl.columns) {
      if (!col.filterable && !col.sortable) continue;
      assertColumnKey(col.key);
      out.set(`ix_tr_${shortHash(`filter:${tpl.key}:${col.key}`)}`, {
        kind: 'filter',
        tableKey: tpl.key,
        columnKey: col.key,
      });
    }
    if (tpl.idField) {
      assertColumnKey(tpl.idField);
      out.set(`ux_tr_${shortHash(`id:${tpl.key}:${tpl.idField}`)}`, {
        kind: 'unique-id',
        tableKey: tpl.key,
        idField: tpl.idField,
      });
    }
    if (tpl.write?.batch?.groupBy?.length) {
      for (const col of tpl.write.batch.groupBy) assertColumnKey(col);
      out.set(`ix_tr_${shortHash(`group:${tpl.key}:${tpl.write.batch.groupBy.join(',')}`)}`, {
        kind: 'group',
        tableKey: tpl.key,
        groupBy: tpl.write.batch.groupBy,
      });
    }
    return out;
  }

  private async ensureIndex(name: string, spec: IndexSpec): Promise<void> {
    assertTableKey(spec.tableKey);

    // A CREATE INDEX CONCURRENTLY that failed partway leaves an INVALID index
    // of the same name behind; IF NOT EXISTS would then silently no-op
    // against the broken index. Drop-and-retry on any leftover invalid index.
    const invalid: unknown[] = await this.dataSource.query(
      `SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = $1 AND NOT i.indisvalid`,
      [name],
    );
    if (invalid.length) await this.dataSource.query(`DROP INDEX CONCURRENTLY IF EXISTS "${name}"`);

    try {
      if (spec.kind === 'filter') {
        await this.dataSource.query(`
          CREATE INDEX CONCURRENTLY IF NOT EXISTS "${name}"
            ON table_rows ((data ->> ${sqlStringLiteral(spec.columnKey)}))
            WHERE table_key = ${sqlStringLiteral(spec.tableKey)}
        `);
      } else if (spec.kind === 'unique-id') {
        await this.dataSource.query(`
          CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "${name}"
            ON table_rows (connection_id, (data ->> ${sqlStringLiteral(spec.idField)}))
            WHERE table_key = ${sqlStringLiteral(spec.tableKey)}
        `);
      } else {
        // Sostiene la re-consulta del sweep de debounce: qué queda `queued` por partición de grupo.
        const groupExprs = spec.groupBy.map((col) => `(data ->> ${sqlStringLiteral(col)})`).join(', ');
        await this.dataSource.query(`
          CREATE INDEX CONCURRENTLY IF NOT EXISTS "${name}"
            ON table_rows (submission_status, ${groupExprs})
            WHERE table_key = ${sqlStringLiteral(spec.tableKey)}
        `);
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (spec.kind === 'unique-id') {
        this.logger.warn(`idField uniqueness violated building "${name}": ${message}`);
        throw new BadRequestException(
          `idField "${spec.idField}" is not unique among existing rows of "${spec.tableKey}" (scoped by connection). ` +
            `Fix or de-duplicate the data before saving this template.`,
        );
      }
      const target = spec.kind === 'filter' ? spec.columnKey : spec.groupBy.join(',');
      this.logger.error(`Failed to create index "${name}": ${message}`);
      throw new InternalServerErrorException(`Failed to create index for column "${target}"`);
    }
  }

  private async dropIndex(name: string): Promise<void> {
    try {
      await this.dataSource.query(`DROP INDEX CONCURRENTLY IF EXISTS "${name}"`);
    } catch (err) {
      // Non-fatal: a leftover index only costs write overhead + disk, never correctness.
      this.logger.error(`Failed to drop index "${name}": ${err instanceof Error ? err.message : String(err)}`);
    }
  }
}
