import { Body, Controller, Delete, Get, Param, Post, Put, UseGuards } from '@nestjs/common';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { SourceConnectionsService } from './source-connections.service';
import { SourceHttpClient } from './source-http.client';
import { UpsertSourceConnectionDto } from './dto/upsert-source-connection.dto';

@UseGuards(MgmtTokenGuard)
@Controller('connections')
export class SourceConnectionsController {
  constructor(
    private readonly service: SourceConnectionsService,
    private readonly client: SourceHttpClient,
  ) {}

  @Get()
  list() {
    return this.service.list();
  }

  @Get(':id')
  async get(@Param('id') id: string) {
    const conn = await this.service.findById(id);
    const { credentialsEncrypted, id: connId, ...rest } = conn;
    return { _id: connId, ...rest };
  }

  @Post()
  create(@Body() dto: UpsertSourceConnectionDto) {
    return this.service.create(dto);
  }

  @Put(':id')
  update(@Param('id') id: string, @Body() dto: UpsertSourceConnectionDto) {
    return this.service.update(id, dto);
  }

  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.service.remove(id);
  }

  /**
   * Real connection test: fire the connection's configured `handshake`
   * (method + path/query/body) through its auth and report the HTTP status. The
   * request body may override the stored handshake for ad-hoc checks; with no
   * handshake and no override it falls back to a bare `GET` on the base URL.
   * This is a reachability/auth check — the table data fetch is configured on
   * each table's `audit`, not here.
   */
  @Post(':id/test')
  async test(
    @Param('id') id: string,
    @Body() body: { path?: string; method?: string; query?: Record<string, string>; requestBody?: unknown },
  ) {
    try {
      const conn = await this.service.resolveById(id);
      const hs = {
        method: body?.method ?? conn.handshake?.method ?? 'GET',
        path: body?.path ?? conn.handshake?.path,
        query: body?.query ?? conn.handshake?.query,
        body: body?.requestBody ?? conn.handshake?.body,
      };
      const { status, statusText } = await this.client.handshake(conn, hs);
      if (status >= 200 && status < 300) {
        return { success: true, message: `OK — ${status} ${statusText}` };
      }
      return { success: false, message: `El sistema respondió ${status} ${statusText}` };
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return { success: false, message };
    }
  }
}
