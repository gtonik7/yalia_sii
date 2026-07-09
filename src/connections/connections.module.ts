import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { SourceConnection } from './entities/source-connection.entity';
import { SourceConnectionsService } from './source-connections.service';
import { SourceConnectionsController } from './source-connections.controller';
import { SourceAuthService } from './source-auth.service';
import { SourceHttpClient } from './source-http.client';

/**
 * Manages external "source" connections (base URL + auth + pagination) and the
 * generic HTTP client the audit poller uses to read from them. Exported so the
 * audit module can resolve a connection and page over it.
 */
@Module({
  imports: [TypeOrmModule.forFeature([SourceConnection])],
  controllers: [SourceConnectionsController],
  providers: [SourceConnectionsService, SourceAuthService, SourceHttpClient],
  exports: [SourceConnectionsService, SourceHttpClient],
})
export class ConnectionsModule {}
