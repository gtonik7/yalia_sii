import { Controller, Get, HttpCode, Param, Post, UseGuards } from '@nestjs/common';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { OperationRunService } from './operation-run.service';
import type { OperationRun } from './entities/operation-run.entity';

/**
 * Estado/resultado de una operación pesada async (borrado masivo, count/stats/report).
 * El FE pollea `GET /v1/operation-runs/:runId` tras recibir `{ runId }` del trigger,
 * hasta que `status` sea `success`/`error`/`canceled`. Proxied por el hub como
 * `GET /satellites/:key/operation-runs/:runId`.
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operation-runs')
export class OperationRunController {
    constructor(private readonly runs: OperationRunService) {}

    @Get(':runId')
    async get(@Param('runId') runId: string): Promise<OperationRun> {
        return this.runs.get(runId);
    }

    @Post(':runId/cancel')
    @HttpCode(200)
    async cancel(@Param('runId') runId: string): Promise<OperationRun> {
        return this.runs.cancel(runId);
    }
}
