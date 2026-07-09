import { Injectable, OnModuleInit } from '@nestjs/common';
import { DatasetRegistryService } from '../datasets/dataset-registry.service';
import {
  DatasetDescriptor,
  DatasetFilterDef,
  DatasetProvider,
  DatasetSource,
  DatasetUpdateParams,
} from '../datasets/dataset.types';
import { TableTemplate } from './entities/table-template.entity';
import { TableTemplatesService } from './table-templates.service';
import { TableRowsService } from './table-rows.service';

/**
 * Exposes every user-managed table template as a generic dataset. The hub proxy
 * and FE explorer need no changes: they consume the same `/v1/datasets` contract.
 * Filter inputs are derived from columns flagged `filterable`, so the existing
 * explorer filter UI works without knowing about templates.
 */
@Injectable()
export class TableDatasetBridge implements DatasetSource, OnModuleInit {
  constructor(
    private readonly templates: TableTemplatesService,
    private readonly rows: TableRowsService,
    private readonly registry: DatasetRegistryService,
  ) {}

  onModuleInit(): void {
    this.registry.registerSource(this);
  }

  async listDescriptors(): Promise<DatasetDescriptor[]> {
    const tpls = await this.templates.findAll();
    return tpls.map((t) => this.toDescriptor(t));
  }

  async resolve(key: string): Promise<DatasetProvider | null> {
    const tpl = await this.templates.findByKey(key);
    if (!tpl) return null;
    return {
      descriptor: this.toDescriptor(tpl),
      query: (params) => this.rows.query(tpl, params),
      deleteRows: (params) => this.rows.deleteRows(tpl, params),
      // Only wired when the template declares `write` — editing is strictly
      // opt-in per table.
      ...(tpl.write
        ? { update: (p: DatasetUpdateParams) => this.rows.updateAndWrite(tpl, p.connectionId, p.id, p.data) }
        : {}),
    };
  }

  private toDescriptor(t: TableTemplate): DatasetDescriptor {
    // Date columns get a "desde"/"hasta" pair instead of one exact-match input,
    // matching the convention every hand-written provider already uses
    // (audit/sent-records: `since`/`until`) so range search works generically.
    const filters: DatasetFilterDef[] = t.columns
      .filter((c) => c.filterable)
      .flatMap((c): DatasetFilterDef[] =>
        c.type === 'date'
          ? [
              { key: `${c.key}_from`, label: `${c.label} (desde)`, type: 'date', column: c.key },
              { key: `${c.key}_until`, label: `${c.label} (hasta)`, type: 'date', column: c.key },
            ]
          : [{ key: c.key, label: c.label, type: c.type }],
      );

    return {
      key: t.key,
      label: t.label,
      description: t.description ?? undefined,
      // Every yalia_sii table's rows are classified by connectionId.
      perConnection: true,
      connectionIds: t.connectionIds?.length ? t.connectionIds : undefined,
      columns: t.columns.map((c) => ({
        key: c.key,
        label: c.label,
        type: c.type,
        filterable: c.filterable,
        sortable: c.sortable,
      })),
      filters: filters.length ? filters : undefined,
      defaultSort: t.defaultSort ?? undefined,
      editable: t.write ? true : undefined,
      deletable: true,
      // Las tablas de usuario se operan desde la pestaña "Registros" del
      // satélite, no desde el Explorer global (donde sí quedan los datasets de
      // diagnóstico: historial de polls/envíos).
      explorerHidden: true,
    };
  }
}
