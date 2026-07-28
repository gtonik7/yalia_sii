import { BadRequestException, Body, Controller, HttpCode, Post, UseGuards } from '@nestjs/common';
import { IsObject, IsOptional, IsString } from 'class-validator';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { TableTemplatesService } from './table-templates.service';
import { OperationRunService } from './operation-run.service';

class TableCountParamsDto {
    @IsOptional()
    @IsString()
    tableKey?: string;

    @IsOptional()
    @IsString()
    connectionId?: string;

    @IsOptional()
    @IsObject()
    filters?: Record<string, string>;
}

class TableCountDto {
    /** Table to count. Accepted at top level or under `params` (hub contract). */
    @IsOptional()
    @IsString()
    tableKey?: string;

    @IsOptional()
    params?: TableCountParamsDto;

    @IsOptional()
    @IsString()
    connectionId?: string;

    @IsOptional()
    @IsObject()
    filters?: Record<string, string>;
}

/**
 * Exact, uncapped row count for a template under the given filters — unlike
 * `query()`'s paginated total (capped, see COUNT_CAP in TableRowsService),
 * this always scans to completion; meant to be called on demand (e.g. before
 * a mass delete, or a "contar exacto" click), not on every list load:
 *   POST /v1/operations/table.count/trigger { params:{ tableKey, filters, connectionId } }
 *   → { runId } (202).
 * El COUNT exacto escanea a fondo y puede superar el techo de timeout del proxy del
 * hub (30 s) en tablas grandes, así que corre en background (`MaintenanceProcessor`);
 * el FE pollea `GET /v1/operation-runs/:runId` → `result.count`.
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operations/table.count')
export class TableCountController {
    constructor(
        private readonly templates: TableTemplatesService,
        private readonly runs: OperationRunService
    ) {}

    @Post('trigger')
    @HttpCode(202)
    async trigger(@Body() dto: TableCountDto): Promise<{ runId: string }> {
        const tableKey = dto.tableKey ?? dto.params?.tableKey;
        if (!tableKey) {
            throw new BadRequestException('tableKey is required (top level or params.tableKey)');
        }
        // Valida que la tabla exista antes de encolar (feedback inmediato).
        await this.templates.getByKey(tableKey);
        const filters = dto.filters ?? dto.params?.filters;
        const connectionId = dto.connectionId ?? dto.params?.connectionId;
        const run = await this.runs.create('table.count', tableKey, { tableKey, connectionId, filters });
        return { runId: run.id };
    }
}
