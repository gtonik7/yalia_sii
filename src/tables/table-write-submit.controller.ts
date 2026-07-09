import { ArrayNotEmpty, IsArray, IsOptional, IsString } from 'class-validator';
import { BadRequestException, Body, Controller, HttpCode, Post, UseGuards } from '@nestjs/common';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { TableWriteBatchService } from './table-write-batch.service';

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
 * → 202. Submits only the rows still eligible (queued/error), partitioned like
 * the queued sweeps; already accepted/pending rows in the selection are skipped.
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operations/table.write.submitRows')
export class TableWriteSubmitController {
  constructor(private readonly service: TableWriteBatchService) {}

  @Post('trigger')
  @HttpCode(202)
  async trigger(@Body() dto: SubmitRowsDto): Promise<{ ok: true; submitted: number; skipped: number }> {
    const tableKey = dto.tableKey ?? dto.params?.tableKey;
    if (!tableKey) {
      throw new BadRequestException('tableKey is required (top level or params.tableKey)');
    }
    const { submitted, skipped } = await this.service.submitByIds(tableKey, dto.ids, dto.connectionId);
    return { ok: true, submitted, skipped };
  }
}
