import { Body, Controller, Get, Param, Put, UseGuards } from '@nestjs/common';
import { IsBoolean, IsInt, IsOptional, Min } from 'class-validator';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { RetentionService } from './retention.service';

class RetentionConfigDto {
  @IsOptional()
  @IsInt()
  @Min(0)
  retentionDays?: number;

  @IsOptional()
  @IsInt()
  @Min(1)
  intervalDays?: number;

  @IsOptional()
  @IsBoolean()
  enabled?: boolean;
}

/**
 * Config + listado (con cuenta atrás y tamaño) de los targets de retención del satélite.
 * El disparo/poll de la purga reusa el camino genérico `v1/operations/retention.purge/trigger`
 * + `v1/operation-runs/:id` (ya proxeados por el hub). Sólo se expone aquí lo específico
 * de retención (targets + su config), proxeado por el hub como `:key/retention/targets`.
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/retention')
export class RetentionController {
  constructor(private readonly retention: RetentionService) {}

  @Get('targets')
  listTargets() {
    return this.retention.listTargets();
  }

  @Put('targets/:key')
  async updateTarget(@Param('key') key: string, @Body() patch: RetentionConfigDto) {
    await this.retention.setConfig(key, patch);
    return { key, updated: true };
  }
}
