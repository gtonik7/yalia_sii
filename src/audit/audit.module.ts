import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { SourcePollRun } from './entities/source-poll-run.entity';
import { SourcePollState } from './entities/source-poll-state.entity';
import { SourcePollRunService } from './source-poll-run.service';
import { SourcePollService } from './source-poll.service';
import { SourcePollController } from './source-poll.controller';
import { SourcePollRunsDatasetProvider } from './source-poll-runs-dataset.provider';
import { ConnectionsModule } from '../connections/connections.module';
import { TablesModule } from '../tables/tables.module';

/**
 * Pull/audit mode: pages an external source (via ConnectionsModule's HTTP client)
 * and upserts rows into a table template (via TablesModule) by its idField. The
 * hub schedules `table.audit.poll`; run history is exposed as a dataset.
 */
@Module({
  imports: [TypeOrmModule.forFeature([SourcePollRun, SourcePollState]), ConnectionsModule, TablesModule],
  controllers: [SourcePollController],
  providers: [SourcePollRunService, SourcePollService, SourcePollRunsDatasetProvider],
})
export class AuditModule {}
