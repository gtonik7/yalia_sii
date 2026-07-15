import { Body, Controller, HttpCode, Post, UseGuards } from '@nestjs/common';
import { IsOptional, IsString } from 'class-validator';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { TableTemplatesService } from './table-templates.service';
import { TableRowsService, TableWriteSummaryEntry } from './table-rows.service';

class TableWriteSummaryDto {
    @IsOptional()
    @IsString()
    connectionId?: string;

    @IsOptional()
    params?: { connectionId?: string };
}

/**
 * Satellite-wide KPI dashboard across every table with a `write` (SII
 * submission) config — counts by derived write status and raw SII
 * submission status, plus the top write-error signatures per table:
 *   POST /v1/operations/table.writeSummary/trigger { connectionId? }
 *   → { generatedAt, tables }.
 * Read-only despite the `trigger` naming — reuses the same
 * `/satellites/:key/operations/:operationKey/trigger` hub proxy as every
 * other triggerable operation (see TableStatsController).
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operations/table.writeSummary')
export class TableWriteSummaryController {
    constructor(
        private readonly templates: TableTemplatesService,
        private readonly rows: TableRowsService
    ) {}

    @Post('trigger')
    @HttpCode(200)
    async trigger(@Body() dto: TableWriteSummaryDto): Promise<{ generatedAt: string; tables: TableWriteSummaryEntry[] }> {
        const connectionId = dto.connectionId ?? dto.params?.connectionId;
        const all = await this.templates.findAll();
        const writeTemplates = all.filter((t) => t.write);
        const tables = await this.rows.getWriteSummary(writeTemplates, connectionId);
        return { generatedAt: new Date().toISOString(), tables };
    }
}
