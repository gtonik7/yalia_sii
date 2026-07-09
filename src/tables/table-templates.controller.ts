import { Body, Controller, Delete, Get, Param, Post, Put, UseGuards } from '@nestjs/common';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { TableTemplatesService } from './table-templates.service';
import { UpsertTableTemplateDto } from './dto/upsert-table-template.dto';
import { TableTemplate } from './entities/table-template.entity';

/** Runtime CRUD for the JSON table templates exposed by this satellite. */
@Controller('v1/tables')
@UseGuards(MgmtTokenGuard)
export class TableTemplatesController {
  constructor(private readonly templates: TableTemplatesService) {}

  @Get()
  list(): Promise<TableTemplate[]> {
    return this.templates.findAll();
  }

  @Get(':key')
  get(@Param('key') key: string): Promise<TableTemplate> {
    return this.templates.getByKey(key);
  }

  @Post()
  create(@Body() dto: UpsertTableTemplateDto): Promise<TableTemplate> {
    return this.templates.create(dto);
  }

  @Put(':key')
  update(@Param('key') key: string, @Body() dto: UpsertTableTemplateDto): Promise<TableTemplate> {
    return this.templates.update(key, dto);
  }

  @Delete(':key')
  remove(@Param('key') key: string): Promise<{ ok: true }> {
    return this.templates.remove(key);
  }
}
