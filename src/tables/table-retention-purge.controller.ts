import { BadRequestException, Body, Controller, HttpCode, Post, UseGuards } from '@nestjs/common';
import { IsOptional, IsString } from 'class-validator';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { OperationRunService } from './operation-run.service';
import { findRetentionTarget } from '../retention/retention-catalog';

class RetentionPurgeParamsDto {
  @IsOptional()
  @IsString()
  targetKey?: string;
}

class RetentionPurgeDto {
  /** Target a purgar. Aceptado a nivel raíz o bajo `params` (contrato del hub). */
  @IsOptional()
  @IsString()
  targetKey?: string;

  @IsOptional()
  params?: RetentionPurgeParamsDto;
}

/**
 * Dispara una purga de retención en background:
 *   POST /v1/operations/retention.purge/trigger { params: { targetKey } } → { runId } (202).
 * El borrado puede recorrer millones de filas (supera el techo de timeout del proxy del
 * hub, 30 s), así que corre en el MaintenanceProcessor con progreso; el FE pollea
 * `GET /v1/operation-runs/:runId` para la barra de avance.
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operations/retention.purge')
export class TableRetentionPurgeController {
  constructor(private readonly runs: OperationRunService) {}

  @Post('trigger')
  @HttpCode(202)
  async trigger(@Body() dto: RetentionPurgeDto): Promise<{ runId: string }> {
    const targetKey = dto.targetKey ?? dto.params?.targetKey;
    if (!targetKey) throw new BadRequestException('targetKey is required (top level or params.targetKey)');
    const def = findRetentionTarget(targetKey);
    if (!def) throw new BadRequestException(`Unknown retention target '${targetKey}'`);
    if (def.readonly || !def.executor) throw new BadRequestException(`El target '${targetKey}' no es purgable`);
    const run = await this.runs.create('retention.purge', targetKey, { targetKey });
    return { runId: run.id };
  }
}
