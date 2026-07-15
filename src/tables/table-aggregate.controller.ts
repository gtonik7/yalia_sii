import { BadRequestException, Body, Controller, HttpCode, Post, UseGuards } from '@nestjs/common';
import { IsArray, IsObject, IsOptional, IsString } from 'class-validator';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { TableTemplatesService } from './table-templates.service';
import { TableRowsService, type TableAggregateGroupBy, type TableAggregateMetric, type TableAggregateResult } from './table-rows.service';

class TableReportParamsDto {
    @IsOptional() @IsString() tableKey?: string;
    @IsOptional() @IsString() connectionId?: string;
    @IsOptional() @IsObject() filters?: Record<string, string>;
    @IsOptional() @IsArray() groupBy?: TableAggregateGroupBy[];
    @IsOptional() @IsArray() metrics?: TableAggregateMetric[];
}

class TableReportDto {
    @IsOptional() @IsString() tableKey?: string;
    @IsOptional() params?: TableReportParamsDto;
    @IsOptional() @IsString() connectionId?: string;
    @IsOptional() @IsObject() filters?: Record<string, string>;
    @IsOptional() @IsArray() groupBy?: TableAggregateGroupBy[];
    @IsOptional() @IsArray() metrics?: TableAggregateMetric[];
}

/**
 * Ad-hoc group-by report over one table's rows, on demand (nothing persisted):
 *   POST /v1/operations/table.report/trigger
 *     { params:{ tableKey, filters, connectionId, groupBy, metrics } }
 *   → TableAggregateResult.
 * Same `tableKey`+`filters`+`connectionId` contract (top level or under `params`)
 * as TableCountController, proxied through the generic hub operation endpoint.
 * Read-only despite the `trigger` naming; the heavy lifting + validation of the
 * groupBy/metrics is in TableRowsService.aggregate().
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operations/table.report')
export class TableAggregateController {
    constructor(
        private readonly templates: TableTemplatesService,
        private readonly rows: TableRowsService
    ) {}

    @Post('trigger')
    @HttpCode(200)
    async trigger(@Body() dto: TableReportDto): Promise<TableAggregateResult> {
        const tableKey = dto.tableKey ?? dto.params?.tableKey;
        if (!tableKey) throw new BadRequestException('tableKey is required (top level or params.tableKey)');
        const template = await this.templates.getByKey(tableKey);
        return this.rows.aggregate(template, {
            connectionId: dto.connectionId ?? dto.params?.connectionId,
            filters: dto.filters ?? dto.params?.filters,
            groupBy: dto.groupBy ?? dto.params?.groupBy ?? [],
            metrics: dto.metrics ?? dto.params?.metrics,
        });
    }
}
