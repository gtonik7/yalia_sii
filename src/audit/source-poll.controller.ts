import { BadRequestException, Body, Controller, Get, HttpCode, Param, Post, Query, UseGuards } from '@nestjs/common';
import { IsOptional, IsString } from 'class-validator';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { SourcePollService } from './source-poll.service';
import { SourcePollRunService } from './source-poll-run.service';

class TriggerAuditDto {
  /** Source connection; optional, falls back to the table's audit.connectionId. */
  @IsString()
  @IsOptional()
  connectionId?: string;

  /** Table to audit. Accepted at top level or under `params` (hub contract). */
  @IsString()
  @IsOptional()
  tableKey?: string;

  @IsOptional()
  params?: { tableKey?: string };
}

/**
 * Triggerable-operation contract (hub = scheduler):
 *   POST /v1/operations/table.audit.poll/trigger { connectionId?, params:{ tableKey } }
 * → { runId }. The cadence lives in the flow origin node; this just runs once.
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operations/table.audit.poll')
export class SourcePollController {
  constructor(
    private readonly poll: SourcePollService,
    private readonly runs: SourcePollRunService,
  ) {}

  @Post('trigger')
  @HttpCode(202)
  async trigger(@Body() dto: TriggerAuditDto) {
    const tableKey = dto.tableKey ?? dto.params?.tableKey;
    if (!tableKey) {
      throw new BadRequestException('tableKey is required (top level or params.tableKey)');
    }
    const runId = await this.poll.poll({
      tableKey,
      connectionId: dto.connectionId,
      trigger: 'manual',
    });
    return { runId };
  }

  @Get('runs')
  listRuns(@Query('tableKey') tableKey?: string, @Query('limit') limit?: string) {
    return this.runs.listByTable(tableKey, Number(limit) || 20);
  }

  @Get('runs/:runId')
  getRun(@Param('runId') runId: string) {
    return this.runs.findById(runId);
  }
}
