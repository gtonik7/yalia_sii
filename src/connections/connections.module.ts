import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { SourceConnection } from './entities/source-connection.entity';
import { SourceConnectionsService } from './source-connections.service';
import { SourceConnectionsController } from './source-connections.controller';
import { SourceAuthService } from './source-auth.service';
import { SourceHttpClient } from './source-http.client';

/**
 * Manages external "source" connections (base URL + auth) and the generic
 * HTTP client used to push outbound writes to them. Exported so the tables
 * module (write submission, internal write cron) can resolve a connection.
 */
@Module({
  imports: [TypeOrmModule.forFeature([SourceConnection])],
  controllers: [SourceConnectionsController],
  providers: [SourceConnectionsService, SourceAuthService, SourceHttpClient],
  exports: [SourceConnectionsService, SourceHttpClient],
})
export class ConnectionsModule {}
