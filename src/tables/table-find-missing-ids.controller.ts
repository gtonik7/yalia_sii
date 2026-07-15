import { BadRequestException, Body, Controller, HttpCode, Post, UseGuards } from '@nestjs/common';
import { ArrayNotEmpty, IsArray, IsOptional, IsString } from 'class-validator';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { TableTemplatesService } from './table-templates.service';
import { TableRowsService } from './table-rows.service';

class FindMissingIdsDto {
  /** Table to check. Accepted at top level or under `params` (hub contract). */
  @IsString()
  @IsOptional()
  tableKey?: string;

  @IsOptional()
  params?: { tableKey?: string };

  /**
   * Required (unlike table.stats' optional/aggregating connectionId): the
   * business-key index this checks against (`ux_tr_<hash>` on
   * `(connection_id, data->>idField)`) is unique per connection, so "is id X
   * missing" is meaningless without one.
   */
  @IsString()
  connectionId!: string;

  /** Business-key ids (template.idField values) to check for presence. */
  @IsArray()
  @ArrayNotEmpty()
  @IsString({ each: true })
  ids!: string[];
}

/**
 * Reconciliation drill-down: given ids the hub knows it sent (from its own
 * audit_events), report which are NOT currently present in table_rows, so an
 * operator can identify the exact missing records instead of just a coarse
 * count:
 *   POST /v1/operations/table.findMissingIds/trigger { params:{ tableKey }, connectionId, ids }
 * → { missingIds, checkedCount, deletedInfo }.
 * Read-only. Requires the template to have `idField` configured — without it
 * there's no business-key concept to diff against.
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operations/table.findMissingIds')
export class TableFindMissingIdsController {
  constructor(
    private readonly templates: TableTemplatesService,
    private readonly rows: TableRowsService,
  ) {}

  @Post('trigger')
  @HttpCode(200)
  async trigger(@Body() dto: FindMissingIdsDto): Promise<{ missingIds: string[]; checkedCount: number; deletedInfo: Record<string, { reason: string; at: string }> }> {
    const tableKey = dto.tableKey ?? dto.params?.tableKey;
    if (!tableKey) {
      throw new BadRequestException('tableKey is required (top level or params.tableKey)');
    }
    const template = await this.templates.getByKey(tableKey);
    if (!template.idField) {
      throw new BadRequestException(`Table "${tableKey}" has no idField configured — cannot diff by business key`);
    }
    return this.rows.findMissingIds(template, dto.connectionId, dto.ids);
  }
}
