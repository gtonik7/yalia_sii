import { BadRequestException, Body, Controller, HttpCode, Post, UseGuards } from '@nestjs/common';
import { IsArray, IsObject, IsOptional, IsString } from 'class-validator';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { TableTemplatesService } from './table-templates.service';
import { OperationRunService } from './operation-run.service';
import type { TableAggregateGroupBy, TableAggregateHaving, TableAggregateMetric } from './table-rows.service';

class TableReportParamsDto {
    @IsOptional() @IsString() tableKey?: string;
    @IsOptional() @IsString() connectionId?: string;
    @IsOptional() @IsObject() filters?: Record<string, string>;
    @IsOptional() @IsArray() groupBy?: TableAggregateGroupBy[];
    @IsOptional() @IsArray() metrics?: TableAggregateMetric[];
    @IsOptional() @IsArray() having?: TableAggregateHaving[];
}

class TableReportDto {
    @IsOptional() @IsString() tableKey?: string;
    @IsOptional() params?: TableReportParamsDto;
    @IsOptional() @IsString() connectionId?: string;
    @IsOptional() @IsObject() filters?: Record<string, string>;
    @IsOptional() @IsArray() groupBy?: TableAggregateGroupBy[];
    @IsOptional() @IsArray() metrics?: TableAggregateMetric[];
    @IsOptional() @IsArray() having?: TableAggregateHaving[];
}

/**
 * Ad-hoc group-by report over one table's rows, on demand (nothing persisted):
 *   POST /v1/operations/table.report/trigger
 *     { params:{ tableKey, filters, connectionId, groupBy, metrics } }
 *   → { runId } (202).
 * El group-by escanea la tabla entera y puede superar el techo de timeout del proxy
 * del hub (30 s), así que corre en background (`MaintenanceProcessor`); el FE pollea
 * `GET /v1/operation-runs/:runId` → `result` (TableAggregateResult). La validación
 * profunda de groupBy/metrics sigue en TableRowsService.aggregate().
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operations/table.report')
export class TableAggregateController {
    constructor(
        private readonly templates: TableTemplatesService,
        private readonly runs: OperationRunService
    ) {}

    @Post('trigger')
    @HttpCode(202)
    async trigger(@Body() dto: TableReportDto): Promise<{ runId: string }> {
        const tableKey = dto.tableKey ?? dto.params?.tableKey;
        if (!tableKey) throw new BadRequestException('tableKey is required (top level or params.tableKey)');
        const groupBy = dto.groupBy ?? dto.params?.groupBy ?? [];
        if (!groupBy.length) throw new BadRequestException('Se requiere al menos una dimensión de agrupación');
        await this.templates.getByKey(tableKey);
        const run = await this.runs.create('table.report', tableKey, {
            tableKey,
            connectionId: dto.connectionId ?? dto.params?.connectionId,
            filters: dto.filters ?? dto.params?.filters,
            groupBy,
            metrics: dto.metrics ?? dto.params?.metrics,
            having: dto.having ?? dto.params?.having,
        });
        return { runId: run.id };
    }
}
