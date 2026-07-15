import { BadRequestException, Body, Controller, Delete, Get, NotFoundException, Param, Patch, Query, UseGuards } from '@nestjs/common';
import { DatasetRegistryService } from './dataset-registry.service';
import { DatasetDescriptor, DatasetPage, DatasetSort, DatasetUpdateResult } from './dataset.types';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';

const RESERVED_PARAMS = new Set(['connectionId', 'page', 'pageSize', 'search', 'sort', 'sortDir']);
const DELETE_RESERVED_PARAMS = new Set(['connectionId', 'ids', 'olderThanDays']);

@Controller('v1/datasets')
@UseGuards(MgmtTokenGuard)
export class DatasetsController {
  constructor(private readonly registry: DatasetRegistryService) {}

  /** List the tables this satellite can expose, with their column metadata. */
  @Get()
  list(): Promise<DatasetDescriptor[]> {
    return this.registry.listDescriptors();
  }

  /** Query one dataset with pagination, optional search, filters and sort. */
  @Get(':key')
  async query(
    @Param('key') key: string,
    @Query() query: Record<string, string>,
  ): Promise<DatasetPage> {
    const provider = await this.registry.resolve(key);

    const page = Math.max(1, parseInt(query.page ?? '1', 10) || 1);
    const pageSize = Math.min(1000, Math.max(1, parseInt(query.pageSize ?? '50', 10) || 50));

    // Anything not reserved is treated as a provider-defined filter.
    const filters: Record<string, string> = {};
    for (const [k, v] of Object.entries(query)) {
      if (!RESERVED_PARAMS.has(k) && v !== undefined && v !== '') filters[k] = v;
    }

    let sort: DatasetSort | undefined;
    if (query.sort) {
      sort = { key: query.sort, dir: query.sortDir === 'desc' ? 'desc' : 'asc' };
    }

    return provider.query({
      connectionId: query.connectionId,
      page,
      pageSize,
      search: query.search || undefined,
      filters: Object.keys(filters).length ? filters : undefined,
      sort,
    });
  }

  /**
   * Full detail of a single row. For datasets whose list omits heavy columns
   * (descriptor.hasDetail), the FE calls this when a row is opened to load the
   * complete record (large jsonb payloads/responses). Opt-in per provider.
   */
  @Get(':key/:id')
  async detail(
    @Param('key') key: string,
    @Param('id') id: string,
    @Query('connectionId') connectionId: string | undefined,
  ): Promise<Record<string, unknown>> {
    const provider = await this.registry.resolve(key);
    if (!provider.getDetail) {
      throw new BadRequestException(`Dataset '${key}' no admite detalle por fila`);
    }
    const row = await provider.getDetail({ id, connectionId });
    if (!row) {
      throw new NotFoundException(`No existe el registro '${id}' en '${key}'`);
    }
    return row;
  }

  /** Delete rows by selection (ids in body) or age (olderThanDays in query). Opt-in per provider. */
  @Delete(':key')
  async remove(
    @Param('key') key: string,
    @Query() query: Record<string, string>,
    @Body() body: { ids?: string[] },
  ): Promise<{ affected: number }> {
    const provider = await this.registry.resolve(key);
    if (!provider.deleteRows) {
      throw new BadRequestException(`Dataset '${key}' no admite borrado`);
    }

    const ids = body?.ids?.length ? body.ids : (query.ids ? query.ids.split(',').filter(Boolean) : undefined);
    const olderThanDays =
      query.olderThanDays !== undefined ? parseInt(query.olderThanDays, 10) : undefined;
    if ((!ids || ids.length === 0) && olderThanDays === undefined) {
      throw new BadRequestException('El borrado requiere ids o olderThanDays');
    }

    const filters: Record<string, string> = {};
    for (const [k, v] of Object.entries(query)) {
      if (!DELETE_RESERVED_PARAMS.has(k) && v !== undefined && v !== '') filters[k] = v;
    }

    return provider.deleteRows({
      connectionId: query.connectionId,
      ids,
      olderThanDays,
      filters: Object.keys(filters).length ? filters : undefined,
    });
  }

  /** Edit one row: local save + optional write-back to the external system. Opt-in per provider. */
  @Patch(':key/:id')
  async update(
    @Param('key') key: string,
    @Param('id') id: string,
    @Query('connectionId') connectionId: string | undefined,
    @Body() body: Record<string, unknown>,
  ): Promise<DatasetUpdateResult> {
    const provider = await this.registry.resolve(key);
    if (!provider.update) {
      throw new BadRequestException(`Dataset '${key}' no admite edición`);
    }
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      throw new BadRequestException('El cuerpo debe ser un objeto JSON');
    }
    return provider.update({ connectionId, id, data: body });
  }
}
