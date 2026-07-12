import { Injectable, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { DatasetRegistryService } from '../datasets/dataset-registry.service';
import type { DatasetDeleteParams, DatasetDescriptor, DatasetPage, DatasetProvider, DatasetQuery } from '../datasets/dataset.types';
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
    columns: [
      { key: 'createdAt', label: 'Inicio', type: 'date', sortable: true, filterable: true },
      { key: 'completedAt', label: 'Fin', type: 'date', filterable: true },
      { key: 'status', label: 'Estado', type: 'string', filterable: true },
      { key: 'tableKey', label: 'Tabla', type: 'string', filterable: true },
      { key: 'connectionName', label: 'Conexión', type: 'string', filterable: true },
      { key: 'trigger', label: 'Disparador', type: 'string', filterable: true },
      { key: 'rowCount', label: 'Filas', type: 'number' },
      { key: 'httpStatus', label: 'HTTP', type: 'number' },
      { key: 'batchId', label: 'Batch', type: 'string' },
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

    const [rows, total] = await qb
      .orderBy('r.createdAt', 'DESC')
      .skip((params.page - 1) * params.pageSize)
      .take(params.pageSize)
      .getManyAndCount();

    return {
      // La ficha genérica del explorer (checkbox/selección/borrado) se
      // engancha por `_id`, no por la PK real de la entidad (`id`).
      rows: rows.map((r) => ({ ...r, _id: r.id })) as unknown as Record<string, unknown>[],
      total,
      page: params.page,
      pageSize: params.pageSize,
    };
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
