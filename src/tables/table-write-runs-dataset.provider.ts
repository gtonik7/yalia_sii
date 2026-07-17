import { Injectable, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, SelectQueryBuilder } from 'typeorm';
import { DatasetRegistryService } from '../datasets/dataset-registry.service';
import type { DatasetDeleteParams, DatasetDescriptor, DatasetDetailParams, DatasetPage, DatasetProvider, DatasetQuery } from '../datasets/dataset.types';
import { TableWriteRun } from './entities/table-write-run.entity';

/**
 * Exposes the outbound-submission (write) run history as a dataset so it shows
 * up in the explorer next to the tables themselves (no FE changes needed).
 * This is the operator's "estado de los envíos": what was submitted, when, by
 * which trigger, and whether the provider ACKed it.
 */
@Injectable()
export class TableWriteRunsDatasetProvider implements DatasetProvider, OnModuleInit {
  readonly descriptor: DatasetDescriptor = {
    key: 'table-write-runs',
    label: 'Ejecuciones de escritura',
    description: 'Historial de lotes salientes de presentación al sistema externo',
    perConnection: false,
    deletable: true,
    // La lista no trae payload_preview/response_body (jsonb ~70-100KB/fila); el
    // FE los carga al abrir el detalle vía getDetail().
    hasDetail: true,
    columns: [
      { key: 'createdAt', label: 'Inicio', type: 'date', sortable: true, filterable: true },
      { key: 'completedAt', label: 'Fin', type: 'date', sortable: true, filterable: true },
      { key: 'status', label: 'Estado', type: 'string', sortable: true, filterable: true },
      { key: 'tableKey', label: 'Tabla', type: 'string', sortable: true, filterable: true },
      { key: 'connectionName', label: 'Conexión', type: 'string', sortable: true, filterable: true },
      { key: 'trigger', label: 'Disparador', type: 'string', sortable: true, filterable: true },
      { key: 'rowCount', label: 'Filas', type: 'number', sortable: true },
      { key: 'httpStatus', label: 'HTTP', type: 'number', sortable: true },
      { key: 'batchId', label: 'Batch', type: 'string', sortable: true },
      // `error_message` es `text` (no jsonb pesado como payload_preview/response_body),
      // así que a diferencia de esos dos sí puede ir en el listado sin problema de peso.
      { key: 'errorMessage', label: 'Error', type: 'string' },
    ],
    filters: [
      {
        key: 'status',
        label: 'Estado',
        type: 'string',
        options: [
          { value: 'sent', label: 'sent (ACK)' },
          { value: 'error', label: 'error' },
        ],
      },
      { key: 'tableKey', label: 'Tabla', type: 'string' },
      { key: 'connectionName', label: 'Conexión', type: 'string' },
      {
        key: 'trigger',
        label: 'Disparador',
        type: 'string',
        options: [
          { value: 'event', label: 'event' },
          { value: 'schedule', label: 'schedule' },
          { value: 'manual', label: 'manual' },
        ],
      },
      { key: 'createdAt_from', label: 'Inicio (desde)', type: 'date', column: 'createdAt' },
      { key: 'createdAt_until', label: 'Inicio (hasta)', type: 'date', column: 'createdAt' },
      { key: 'completedAt_from', label: 'Fin (desde)', type: 'date', column: 'completedAt' },
      { key: 'completedAt_until', label: 'Fin (hasta)', type: 'date', column: 'completedAt' },
    ],
    defaultSort: { key: 'createdAt', dir: 'desc' },
  };

  constructor(
    @InjectRepository(TableWriteRun) private readonly repo: Repository<TableWriteRun>,
    private readonly registry: DatasetRegistryService,
  ) {}

  onModuleInit(): void {
    this.registry.register(this);
  }

  async query(params: DatasetQuery): Promise<DatasetPage> {
    const qb = this.repo.createQueryBuilder('r');
    if (params.filters?.tableKey) qb.andWhere('r.tableKey = :tableKey', { tableKey: params.filters.tableKey });
    if (params.filters?.status) qb.andWhere('r.status = :status', { status: params.filters.status });
    if (params.filters?.trigger) qb.andWhere('r.trigger = :trigger', { trigger: params.filters.trigger });
    if (params.filters?.connectionName) qb.andWhere('r.connectionName ILIKE :connectionName', { connectionName: `%${params.filters.connectionName}%` });
    if (params.filters?.createdAt_from) qb.andWhere('r.createdAt >= :createdAtFrom', { createdAtFrom: params.filters.createdAt_from });
    if (params.filters?.createdAt_until) qb.andWhere('r.createdAt <= :createdAtUntil', { createdAtUntil: params.filters.createdAt_until });
    if (params.filters?.completedAt_from) qb.andWhere('r.completedAt >= :completedAtFrom', { completedAtFrom: params.filters.completedAt_from });
    if (params.filters?.completedAt_until) qb.andWhere('r.completedAt <= :completedAtUntil', { completedAtUntil: params.filters.completedAt_until });

    // Orden pedido por el FE, validado contra las columnas marcadas `sortable`
    // en el descriptor; si no viene o no es válido, más recientes primero.
    const sortableKeys = new Set(this.descriptor.columns.filter((c) => c.sortable).map((c) => c.key));
    const sortDir = params.sort?.dir === 'asc' ? 'ASC' : 'DESC';
    const sortKey = params.sort && sortableKeys.has(params.sort.key) ? params.sort.key : 'createdAt';

    const [rows, total] = await Promise.all([
      qb
        .clone()
        // Solo la PK + las columnas de la grilla. Se excluyen adrede
        // payload_preview y response_body (jsonb ~70-100KB/fila): traerlos aquí
        // hacía que una página de 1000 filas pesara decenas de MB y expirara el
        // timeout del proxy del hub. El detalle completo se carga bajo demanda
        // en getDetail() al abrir la fila.
        .select(['r.id', ...this.descriptor.columns.map((c) => `r.${c.key}`)])
        .orderBy(`r.${sortKey}`, sortDir)
        .skip((params.page - 1) * params.pageSize)
        .take(params.pageSize)
        .getMany(),
      this.countTotal(qb, params.filters),
    ]);

    return {
      // La ficha genérica del explorer (checkbox/selección/borrado) se
      // engancha por `_id`, no por la PK real de la entidad (`id`).
      rows: rows.map((r) => ({ ...r, _id: r.id })) as unknown as Record<string, unknown>[],
      total,
      page: params.page,
      pageSize: params.pageSize,
    };
  }

  /**
   * Fila completa (incluye payload_preview/response_body) para el detalle que
   * el FE abre al hacer clic. Es una única fila por PK, así que el peso del
   * jsonb no es problema aquí (a diferencia de traerlo por página en la lista).
   */
  async getDetail(params: DatasetDetailParams): Promise<Record<string, unknown> | null> {
    const row = await this.repo.findOne({ where: { id: params.id } });
    if (!row) return null;
    return { ...row, _id: row.id } as unknown as Record<string, unknown>;
  }

  /**
   * `table_write_runs` es hypertable comprimida a partir de 7 días
   * (TableWriteRunsCompression1751000016000) con retention de 365 días: un
   * COUNT(*) exacto sin filtro obliga a descomprimir todos los chunks del
   * histórico para contarlos, ignorando el LIMIT — eso es lo que hacía
   * tardar muchísimo (y a veces expirar el timeout del axios del FE) al abrir
   * la pestaña sin filtrar, que es el caso por defecto. Sin filtros se usa el
   * conteo aproximado de Timescale (metadata de catálogo, no descomprime
   * nada); en cuanto se aplica cualquier filtro se mantiene el COUNT exacto
   * de siempre, ya acotado por ese filtro.
   */
  private async countTotal(qb: SelectQueryBuilder<TableWriteRun>, filters?: Record<string, string>): Promise<number> {
    if (filters && Object.keys(filters).length > 0) {
      return qb.clone().getCount();
    }
    try {
      const result: Array<{ count: string }> = await this.repo.query(`SELECT hypertable_approximate_row_count('table_write_runs') AS count`);
      return Number(result[0]?.count ?? 0);
    } catch {
      return qb.clone().getCount();
    }
  }

  async deleteRows(params: DatasetDeleteParams): Promise<{ affected: number }> {
    const qb = this.repo.createQueryBuilder().delete();

    if (params.ids && params.ids.length > 0) {
      qb.whereInIds(params.ids);
    } else if (params.olderThanDays) {
      const cutoff = new Date();
      cutoff.setDate(cutoff.getDate() - params.olderThanDays);
      qb.where('createdAt < :cutoff', { cutoff });
    } else {
      return { affected: 0 };
    }

    const result = await qb.execute();
    return { affected: result.affected ?? 0 };
  }
}
