import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import type { Job } from 'bullmq';
import { QUEUES } from '../core/queues/queues.constants';
import { OperationRunService, type MaintenanceJobData } from './operation-run.service';
import { TableTemplatesService } from './table-templates.service';
import { TableRowsService, type TableAggregateGroupBy, type TableAggregateHaving, type TableAggregateMetric } from './table-rows.service';

/**
 * Ejecuta en background las operaciones pesadas de tabla encoladas por sus controllers
 * (borrado masivo, count/stats/report exactos), escribiendo el desenlace en `operation_runs`.
 * La lógica de negocio vive intacta en `TableRowsService`; aquí sólo se despacha por
 * `operation` y se traduce a resultado. Los parámetros ya fueron validados por el controller
 * antes de encolar (confirm/allowBulkDelete/filtros no vacíos, etc.).
 */
@Processor(QUEUES.MAINTENANCE, { concurrency: Number(process.env.MAINTENANCE_CONCURRENCY) || 2 })
export class MaintenanceProcessor extends WorkerHost {
    private readonly logger = new Logger(MaintenanceProcessor.name);

    constructor(
        private readonly runs: OperationRunService,
        private readonly templates: TableTemplatesService,
        private readonly rows: TableRowsService,
    ) {
        super();
    }

    async process(job: Job<MaintenanceJobData>): Promise<void> {
        const { runId } = job.data;
        const run = await this.runs.find(runId);
        // Cancelado antes de arrancar (o borrado): no-op.
        if (!run || run.status === 'canceled') return;

        await this.runs.markRunning(runId);
        try {
            const result = await this.dispatch(run.operation, run.params);
            await this.runs.markSuccess(runId, result);
            this.logger.log(`Maintenance op "${run.operation}" run=${runId} ok`);
        } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            await this.runs.markError(runId, message);
            this.logger.error(`Maintenance op "${run.operation}" run=${runId} failed: ${message}`);
            // No re-lanzar: el desenlace queda en la fila; el job se completa sin reintentos
            // (attempts:1 igualmente). Re-lanzar sólo llenaría los logs de BullMQ.
        }
    }

    private async dispatch(operation: string, params: Record<string, unknown>): Promise<Record<string, unknown>> {
        const tableKey = String(params.tableKey ?? '');
        const template = await this.templates.getByKey(tableKey);
        const connectionId = params.connectionId as string | undefined;
        const filters = params.filters as Record<string, string> | undefined;

        switch (operation) {
            case 'table.bulkDelete': {
                const nonEmptyFilters = Object.fromEntries(Object.entries(filters ?? {}).filter(([, v]) => v !== ''));
                const { affected } = await this.rows.deleteRows(template, { connectionId, filters: nonEmptyFilters });
                return { deletedCount: affected };
            }
            case 'table.count': {
                const count = await this.rows.countFiltered(template, filters, connectionId);
                return { count };
            }
            case 'table.stats': {
                const stats = await this.rows.getStats(template, connectionId);
                return stats as unknown as Record<string, unknown>;
            }
            case 'table.report': {
                const result = await this.rows.aggregate(template, {
                    connectionId,
                    filters,
                    groupBy: (params.groupBy as TableAggregateGroupBy[] | undefined) ?? [],
                    metrics: params.metrics as TableAggregateMetric[] | undefined,
                    having: params.having as TableAggregateHaving[] | undefined,
                });
                return result as unknown as Record<string, unknown>;
            }
            default:
                throw new Error(`Unknown maintenance operation "${operation}"`);
        }
    }
}
