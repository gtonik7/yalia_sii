import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { BullModule } from '@nestjs/bullmq';
import { TableTemplate } from './entities/table-template.entity';
import { TableRow } from './entities/table-row.entity';
import { TableWriteRun } from './entities/table-write-run.entity';
import { TableDeleteEvent } from './entities/table-delete-event.entity';
import { OperationRun } from './entities/operation-run.entity';
import { TableTemplatesService } from './table-templates.service';
import { TableRowsService } from './table-rows.service';
import { TableIndexManagerService } from './table-index-manager.service';
import { TableTemplatesController } from './table-templates.controller';
import { TableDatasetBridge } from './table-dataset.bridge';
import { IngestTableOperation } from './ingest-table.operation';
import { WriteCronService } from './write-cron.service';
import { TableRetentionCron } from './table-retention.cron';
import { StaleLoadCron } from './stale-load.cron';
import { TableWriteBatchService } from './table-write-batch.service';
import { TableWriteBatchController } from './table-write-batch.controller';
import { TableWriteSubmitController } from './table-write-submit.controller';
import { TableStatsController } from './table-stats.controller';
import { TableResetDeleteBaselineController } from './table-reset-delete-baseline.controller';
import { TableFindMissingIdsController } from './table-find-missing-ids.controller';
import { TableBulkDeleteController } from './table-bulk-delete.controller';
import { TableWriteSummaryController } from './table-write-summary.controller';
import { TableCountController } from './table-count.controller';
import { TableAggregateController } from './table-aggregate.controller';
import { TableWriteRunService } from './table-write-run.service';
import { TableWriteRunsDatasetProvider } from './table-write-runs-dataset.provider';
import { OperationRunService } from './operation-run.service';
import { OperationRunController } from './operation-run.controller';
import { MaintenanceProcessor } from './maintenance.processor';
import { TableRetentionPurgeController } from './table-retention-purge.controller';
import { RetentionSchedulerService } from './retention-scheduler.service';
import { OperationRegistryModule } from '../operations/operation-registry.module';
import { ConnectionsModule } from '../connections/connections.module';
import { OutboxModule } from '../outbox/outbox.module';
import { RetentionModule } from '../retention/retention.module';
import { QUEUES } from '../core/queues/queues.constants';

/**
 * Generic, template-driven data tables: CRUD of JSON templates, ingest of rows
 * via the `table.ingest` flow operation, and exposure of each template as a
 * dataset (DatasetsModule is @Global, so its registry is injectable here).
 */
@Module({
    imports: [
        TypeOrmModule.forFeature([TableTemplate, TableRow, TableWriteRun, TableDeleteEvent, OperationRun]),
        OperationRegistryModule,
        ConnectionsModule,
        OutboxModule,
        RetentionModule,
        BullModule.registerQueue({ name: QUEUES.MAINTENANCE }),
    ],
    controllers: [
        TableTemplatesController,
        TableWriteBatchController,
        TableWriteSubmitController,
        TableStatsController,
        TableResetDeleteBaselineController,
        TableFindMissingIdsController,
        TableBulkDeleteController,
        TableWriteSummaryController,
        TableCountController,
        TableAggregateController,
        OperationRunController,
        TableRetentionPurgeController,
    ],
    providers: [
        TableTemplatesService,
        TableRowsService,
        TableIndexManagerService,
        TableDatasetBridge,
        IngestTableOperation,
        WriteCronService,
        TableRetentionCron,
        StaleLoadCron,
        TableWriteBatchService,
        TableWriteRunService,
        TableWriteRunsDatasetProvider,
        OperationRunService,
        MaintenanceProcessor,
        RetentionSchedulerService,
    ],
    exports: [TableTemplatesService, TableRowsService],
})
export class TablesModule {}
