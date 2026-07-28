import { BadRequestException, Body, Controller, ForbiddenException, HttpCode, Post, UseGuards } from '@nestjs/common';
import { IsBoolean, IsObject, IsOptional, IsString } from 'class-validator';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { TableTemplatesService } from './table-templates.service';
import { OperationRunService } from './operation-run.service';

class TableBulkDeleteParamsDto {
    @IsOptional()
    @IsString()
    tableKey?: string;

    @IsOptional()
    @IsString()
    connectionId?: string;

    @IsOptional()
    @IsObject()
    filters?: Record<string, string>;

    /** Explicit safety net in addition to the FE's own confirmation step. */
    @IsOptional()
    @IsBoolean()
    confirm?: boolean;
}

class TableBulkDeleteDto {
    /** Table to delete from. Accepted at top level or under `params` (hub contract). */
    @IsString()
    @IsOptional()
    tableKey?: string;

    @IsOptional()
    params?: TableBulkDeleteParamsDto;

    @IsOptional()
    @IsString()
    connectionId?: string;

    @IsOptional()
    @IsObject()
    filters?: Record<string, string>;

    @IsOptional()
    @IsBoolean()
    confirm?: boolean;
}

/**
 * Controlled mass delete, gated per template (`allowBulkDelete`, off by
 * default) and requiring at least one non-empty filter — this operation must
 * never be able to wipe a whole table:
 *   POST /v1/operations/table.bulkDelete/trigger
 *     { params:{ tableKey, filters, connectionId, confirm:true } }
 *   → { runId } (202).
 * El borrado real corre en background (`MaintenanceProcessor`) porque un DELETE
 * sobre `table_rows` (compartida, >1M filas) puede superar el techo de timeout del
 * proxy del hub (30 s) — y una desconexión NO cancela el DELETE, dejando al usuario
 * sin saber el desenlace. La validación (confirm/allowBulkDelete/filtros) es síncrona
 * para dar feedback inmediato; sólo la ejecución se difiere. El FE pollea
 * `GET /v1/operation-runs/:runId` → `result.deletedCount`.
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operations/table.bulkDelete')
export class TableBulkDeleteController {
    constructor(
        private readonly templates: TableTemplatesService,
        private readonly runs: OperationRunService
    ) {}

    @Post('trigger')
    @HttpCode(202)
    async trigger(@Body() dto: TableBulkDeleteDto): Promise<{ runId: string }> {
        const tableKey = dto.tableKey ?? dto.params?.tableKey;
        if (!tableKey) {
            throw new BadRequestException('tableKey is required (top level or params.tableKey)');
        }
        const confirm = dto.confirm ?? dto.params?.confirm;
        if (confirm !== true) {
            throw new BadRequestException('confirm debe ser true para ejecutar un borrado masivo');
        }
        const filters = dto.filters ?? dto.params?.filters;
        const nonEmptyFilters = Object.fromEntries(Object.entries(filters ?? {}).filter(([, v]) => v !== ''));
        if (Object.keys(nonEmptyFilters).length === 0) {
            throw new BadRequestException('El borrado masivo requiere al menos un filtro no vacío');
        }

        const template = await this.templates.getByKey(tableKey);
        if (!template.allowBulkDelete) {
            throw new ForbiddenException(`El borrado masivo no está habilitado para la tabla "${tableKey}"`);
        }

        const connectionId = dto.connectionId ?? dto.params?.connectionId;
        const run = await this.runs.create('table.bulkDelete', tableKey, { tableKey, connectionId, filters: nonEmptyFilters });
        return { runId: run.id };
    }
}
