import { BadRequestException, Body, Controller, HttpCode, Post, UseGuards } from '@nestjs/common';
import { IsIn, IsOptional, IsString } from 'class-validator';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { TableWriteBatchService } from './table-write-batch.service';

class TriggerBatchSubmitDto {
  /** Table to sweep. Accepted at top level or under `params` (hub contract). */
  @IsString()
  @IsOptional()
  tableKey?: string;

  @IsOptional()
  params?: { tableKey?: string };

  /**
   * How the sweep was invoked — only for run-history labelling. Defaults to
   * `schedule` (the hub cron path); the FE "Forzar envío" button sends
   * `manual`. Never changes what gets submitted, only how the run is tagged.
   */
  @IsOptional()
  @IsIn(['schedule', 'manual'])
  trigger?: 'schedule' | 'manual';

  /**
   * Source connection to scope the sweep to — the hub forwards this verbatim
   * from the Flow origin's `connectionId`, the same field already used to
   * filter every other hub-scheduled operation.
   */
  @IsString()
  @IsOptional()
  connectionId?: string;
}

/**
 * Triggerable-operation contract (hub = scheduler):
 *   POST /v1/operations/table.write.batchSubmit/trigger { params:{ tableKey }, connectionId }
 * → 202. Sweeps every `queued` row of the template (optionally scoped to one
 * connection), partitioned by `write.batch.groupBy`, one outbound
 * submitGroup() call per partition/chunk.
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operations/table.write.batchSubmit')
export class TableWriteBatchController {
  constructor(private readonly service: TableWriteBatchService) {}

  @Post('trigger')
  @HttpCode(202)
  async trigger(@Body() dto: TriggerBatchSubmitDto): Promise<{ ok: true; queued: number }> {
    const tableKey = dto.tableKey ?? dto.params?.tableKey;
    if (!tableKey) {
      throw new BadRequestException('tableKey is required (top level or params.tableKey)');
    }
    const { queued } = await this.service.trigger(tableKey, dto.trigger ?? 'schedule', dto.connectionId);
    return { ok: true, queued };
  }
}
