import { BadRequestException, Body, Controller, HttpCode, Post, UseGuards } from '@nestjs/common';
import { IsOptional, IsString } from 'class-validator';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { TableTemplatesService } from './table-templates.service';
import { OperationRunService } from './operation-run.service';

class TableStatsDto {
  /** Table to report on. Accepted at top level or under `params` (hub contract). */
  @IsString()
  @IsOptional()
  tableKey?: string;

  @IsOptional()
  params?: { tableKey?: string };

  /** Scope the stats to one source connection; omitted = every connection. */
  @IsString()
  @IsOptional()
  connectionId?: string;
}

/**
 * Reconciliation stats for one table, used by the hub-fe "Conciliación" page
 * to tell expected dedup collapse apart from unexplained loss:
 *   POST /v1/operations/table.stats/trigger { params:{ tableKey }, connectionId }
 * → { runId } (202).
 * El `count(DISTINCT …)` sobre tablas grandes puede superar el techo de timeout del
 * proxy del hub (30 s), así que corre en background (`MaintenanceProcessor`); el FE
 * pollea `GET /v1/operation-runs/:runId` → `result` (las stats de conciliación).
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operations/table.stats')
export class TableStatsController {
  constructor(
    private readonly templates: TableTemplatesService,
    private readonly runs: OperationRunService,
  ) {}

  @Post('trigger')
  @HttpCode(202)
  async trigger(@Body() dto: TableStatsDto): Promise<{ runId: string }> {
    const tableKey = dto.tableKey ?? dto.params?.tableKey;
    if (!tableKey) {
      throw new BadRequestException('tableKey is required (top level or params.tableKey)');
    }
    await this.templates.getByKey(tableKey);
    const run = await this.runs.create('table.stats', tableKey, { tableKey, connectionId: dto.connectionId });
    return { runId: run.id };
  }
}
