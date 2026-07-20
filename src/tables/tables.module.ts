import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { BullModule } from '@nestjs/bullmq';
import { TableTemplate } from './entities/table-template.entity';
import { TableRow } from './entities/table-row.entity';
import { TableWriteRun } from './entities/table-write-run.entity';
import { TableDeleteEvent } from './entities/table-delete-event.entity';
import { TableTemplatesService } from './table-templates.service';
import { TableRowsService } from './table-rows.service';
import { TableIndexManagerService } from './table-index-manager.service';
import { TableTemplatesController } from './table-templates.controller';
import { TableDatasetBridge } from './table-dataset.bridge';
import { IngestTableOperation } from './ingest-table.operation';
import { WriteEventProcessor } from './write-event.processor';
import { WriteCronService } from './write-cron.service';
import { TableRetentionCron } from './table-retention.cron';
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
import { OperationRegistryModule } from '../operations/operation-registry.module';
import { ConnectionsModule } from '../connections/connections.module';
import { OutboxModule } from '../outbox/outbox.module';
import { QUEUES } from '../core/queues/queues.constants';

/**
 * Generic, template-driven data tables: CRUD of JSON templates, ingest of rows
 * via the `table.ingest` flow operation, and exposure of each template as a
 * dataset (DatasetsModule is @Global, so its registry is injectable here).
 */
@Module({
    imports: [TypeOrmModule.forFeature([TableTemplate, TableRow, TableWriteRun, TableDeleteEvent]), OperationRegistryModule, ConnectionsModule, OutboxModule, BullModule.registerQueue({ name: QUEUES.WRITE_EVENT })],
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
    ],
    providers: [
        TableTemplatesService,
        TableRowsService,
        TableIndexManagerService,
        TableDatasetBridge,
        IngestTableOperation,
        WriteEventProcessor,
        WriteCronService,
        TableRetentionCron,
        TableWriteBatchService,
        TableWriteRunService,
        TableWriteRunsDatasetProvider,
    ],
    exports: [TableTemplatesService, TableRowsService],
})
export class TablesModule {}
