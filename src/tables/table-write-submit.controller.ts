import { ArrayNotEmpty, IsArray, IsOptional, IsString } from 'class-validator';
import { BadRequestException, Body, Controller, HttpCode, Post, UseGuards } from '@nestjs/common';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { OperationRunService } from './operation-run.service';

class SubmitRowsDto {
  /** Table the selected rows belong to. Accepted at top level or under `params` (hub contract). */
  @IsString()
  @IsOptional()
  tableKey?: string;

  @IsOptional()
  params?: { tableKey?: string };

  /** Ids of the rows to force-submit. Only queued/error rows are actually sent (the rest are skipped). */
  @IsArray()
  @ArrayNotEmpty()
  @IsString({ each: true })
  ids!: string[];

  /** Source connection to scope the selection to. */
  @IsString()
  @IsOptional()
  connectionId?: string;
}

/**
 * Manual "force submit" of a checked selection of rows (FE bulk action),
 * counterpart to `table.write.batchSubmit` (which sweeps every queued row):
 *   POST /v1/operations/table.write.submitRows/trigger { params:{ tableKey }, ids, connectionId }
 * → { runId } (202). The actual submission (group-expansion + one or more
 * outbound HTTP calls per partition, via `TableWriteBatchService.submitByIds`)
 * runs in background through `MaintenanceProcessor`, same as `table.bulkDelete`:
 * with the FE checkbox now cascading a whole `_groupId` group on one click, a
 * selection can mean enough outbound calls to blow past the hub's proxy
 * timeout if awaited synchronously in the request. The FE polls
 * `GET /v1/operation-runs/:runId` for `{ submitted, skipped }`.
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operations/table.write.submitRows')
export class TableWriteSubmitController {
  constructor(private readonly runs: OperationRunService) {}

  @Post('trigger')
  @HttpCode(202)
  async trigger(@Body() dto: SubmitRowsDto): Promise<{ runId: string }> {
    const tableKey = dto.tableKey ?? dto.params?.tableKey;
    if (!tableKey) {
      throw new BadRequestException('tableKey is required (top level or params.tableKey)');
    }
    const run = await this.runs.create('table.write.submitRows', tableKey, {
      tableKey,
      ids: dto.ids,
      connectionId: dto.connectionId,
    });
    return { runId: run.id };
  }
}
