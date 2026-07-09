import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { TableTemplate } from './entities/table-template.entity';
import { UpsertTableTemplateDto } from './dto/upsert-table-template.dto';
import { TableIndexManagerService } from './table-index-manager.service';

@Injectable()
export class TableTemplatesService {
  constructor(
    @InjectRepository(TableTemplate)
    private readonly repo: Repository<TableTemplate>,
    private readonly indexes: TableIndexManagerService,
  ) {}

  /** Reject templates whose idField/defaultSort/duplicate columns are incoherent. */
  private validate(dto: UpsertTableTemplateDto): void {
    if (!dto.columns.length) {
      throw new BadRequestException('A template needs at least one column');
    }
    const keys = new Set<string>();
    for (const c of dto.columns) {
      if (keys.has(c.key)) throw new BadRequestException(`Duplicate column key "${c.key}"`);
      keys.add(c.key);
    }
    if (dto.idField && !keys.has(dto.idField)) {
      throw new BadRequestException(`idField "${dto.idField}" is not one of the columns`);
    }
    if (dto.defaultSort) {
      if (!keys.has(dto.defaultSort.key)) {
        throw new BadRequestException(`defaultSort.key "${dto.defaultSort.key}" is not one of the columns`);
      }
      const col = dto.columns.find((c) => c.key === dto.defaultSort!.key);
      if (col && !col.sortable) {
        throw new BadRequestException(`defaultSort.key "${dto.defaultSort.key}" must be a sortable column`);
      }
    }
    if (dto.write?.path.includes('{id}') && !dto.idField) {
      // Without idField, {id} would resolve against the internal row id,
      // which is almost never a valid identifier for the external system.
      throw new BadRequestException(
        'write.path uses {id} but the table has no idField — it would resolve against the internal row id, not a valid external identifier',
      );
    }
    if (dto.write?.batch) {
      for (const groupKey of dto.write.batch.groupBy) {
        if (!keys.has(groupKey)) {
          throw new BadRequestException(`write.batch.groupBy "${groupKey}" is not one of the columns`);
        }
      }
    }
  }

  findAll(): Promise<TableTemplate[]> {
    return this.repo.find({ order: { label: 'ASC' } });
  }

  findByKey(key: string): Promise<TableTemplate | null> {
    return this.repo.findOne({ where: { key } });
  }

  async getByKey(key: string): Promise<TableTemplate> {
    const tpl = await this.findByKey(key);
    if (!tpl) throw new NotFoundException(`Template "${key}" not found`);
    return tpl;
  }

  async create(dto: UpsertTableTemplateDto): Promise<TableTemplate> {
    this.validate(dto);
    if (await this.repo.exists({ where: { key: dto.key } })) {
      throw new BadRequestException(`Template "${dto.key}" already exists`);
    }
    const saved = await this.repo.save(this.repo.create(this.toEntity(dto)));
    await this.indexes.syncIndexes(null, saved);
    return saved;
  }

  async update(key: string, dto: UpsertTableTemplateDto): Promise<TableTemplate> {
    this.validate(dto);
    const existing = await this.getByKey(key);
    if (dto.key !== key && (await this.repo.exists({ where: { key: dto.key } }))) {
      throw new BadRequestException(`Template "${dto.key}" already exists`);
    }
    const saved = await this.repo.save(this.repo.merge(this.repo.create(existing), this.toEntity(dto)));
    await this.indexes.syncIndexes(existing, saved);
    return saved;
  }

  async remove(key: string): Promise<{ ok: true }> {
    const existing = await this.getByKey(key);
    await this.indexes.dropAllIndexes(existing);
    const res = await this.repo.delete({ key });
    if (res.affected === 0) throw new NotFoundException(`Template "${key}" not found`);
    return { ok: true };
  }

  private toEntity(dto: UpsertTableTemplateDto): Partial<TableTemplate> {
    return {
      key: dto.key,
      label: dto.label,
      description: dto.description ?? null,
      connectionIds: dto.connectionIds?.length ? dto.connectionIds : null,
      idField: dto.idField ?? '',
      columns: dto.columns.map((c) => ({
        key: c.key,
        label: c.label,
        type: c.type,
        filterable: c.filterable ?? false,
        sortable: c.sortable ?? false,
      })),
      defaultSort: dto.defaultSort ?? null,
      write: dto.write
        ? {
            connectionId: dto.write.connectionId,
            method: dto.write.method,
            path: dto.write.path,
            query: dto.write.query,
            externalRefPath: dto.write.externalRefPath,
            trigger: dto.write.trigger,
            batch: dto.write.batch,
          }
        : null,
    };
  }
}
