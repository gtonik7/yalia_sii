import { BadRequestException, Body, Controller, HttpCode, Post, UseGuards } from '@nestjs/common';
import { IsOptional, IsString } from 'class-validator';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { TableTemplatesService } from './table-templates.service';
import { TableRowsService } from './table-rows.service';

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
 * → { rowCount, distinctIds, deletedSinceLoad, voluntaryDeletes, uncontrolledDeletes, missingRecency }.
 * Read-only despite the `trigger` naming — reuses the same
 * `/satellites/:key/operations/:operationKey/trigger` hub proxy as every other
 * triggerable operation (see TableWriteBatchController), so the FE needs no
 * new hub route to reach it.
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operations/table.stats')
export class TableStatsController {
  constructor(
    private readonly templates: TableTemplatesService,
    private readonly rows: TableRowsService,
  ) {}

  @Post('trigger')
  @HttpCode(200)
  async trigger(
    @Body() dto: TableStatsDto
  ): Promise<{ rowCount: number; distinctIds: number | null; deletedSinceLoad: number; voluntaryDeletes: number; uncontrolledDeletes: number; missingRecency: number | null }> {
    const tableKey = dto.tableKey ?? dto.params?.tableKey;
    if (!tableKey) {
      throw new BadRequestException('tableKey is required (top level or params.tableKey)');
    }
    const template = await this.templates.getByKey(tableKey);
    return this.rows.getStats(template, dto.connectionId);
  }
}
