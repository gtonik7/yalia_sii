export type DatasetColumnType = 'string' | 'number' | 'date' | 'boolean' | 'json';

export interface DatasetColumn {
  /** Key in each row object. */
  key: string;
  /** Human label for the column header. */
  label: string;
  type: DatasetColumnType;
  /** When true, the FE offers a filter input for this column. */
  filterable?: boolean;
  /** When true, the FE header is clickable to sort by this column (server-side). */
  sortable?: boolean;
}

export interface DatasetFilterDef {
  key: string;
  label: string;
  type: DatasetColumnType;
  /** Fixed options for select-style filters (e.g. status). */
  options?: Array<{ value: string; label: string }>;
  /**
   * Column this filter narrows, for FE grouping (e.g. show one funnel toggle
   * per column instead of one per filter). Defaults to `key` when omitted —
   * only needed when the filter key differs from the column key, like a
   * `since`/`until` pair that both filter the `createdAt` column.
   */
  column?: string;
}

export interface DatasetSort {
  key: string;
  dir: 'asc' | 'desc';
}

/**
 * Self-describing metadata a satellite publishes about a queryable table.
 * The hub forwards it verbatim and the FE renders generically from `columns`,
 * so no concrete-system knowledge leaks out of the satellite.
 */
export interface DatasetDescriptor {
  /** Stable identifier used in the query URL (e.g. 'mi-tabla'). */
  key: string;
  label: string;
  description?: string;
  /** When true, queries require a connectionId. */
  perConnection: boolean;
  /**
   * When perConnection, the explorer limits its connection picker to these ids.
   * Empty/absent = every connection is offered.
   */
  connectionIds?: string[];
  columns: DatasetColumn[];
  filters?: DatasetFilterDef[];
  defaultSort?: DatasetSort;
  /** When true, the provider exposes deleteRows() and the FE shows delete controls. */
  deletable?: boolean;
  /** When true, the provider exposes update() and the FE shows an edit form. */
  editable?: boolean;
  /**
   * When true, the FE hides this dataset from the global Explorer (it stays
   * reachable through its own dedicated tab — e.g. las tablas de usuario, que
   * se operan desde la pestaña "Registros"). Los datasets de diagnóstico no lo
   * ponen y siguen visibles en el Explorer.
   */
  explorerHidden?: boolean;
}

export interface DatasetQuery {
  connectionId?: string;
  page: number;
  pageSize: number;
  search?: string;
  /** Arbitrary key/value filters; the provider decides which it honors. */
  filters?: Record<string, string>;
  /** Column key + direction; the provider validates the key is sortable. */
  sort?: DatasetSort;
}

export interface DatasetPage {
  rows: Record<string, unknown>[];
  total: number;
  page: number;
  pageSize: number;
}

/**
 * Criteria for deleting rows from a dataset. Either `ids` (row-level selection)
 * or `olderThanDays` (age-based purge) must be provided; `connectionId` scopes
 * the deletion so it never touches another connection's rows.
 */
export interface DatasetDeleteParams {
  connectionId?: string;
  ids?: string[];
  olderThanDays?: number;
  filters?: Record<string, string>;
}

/** Params for editing a single row of an editable dataset. */
export interface DatasetUpdateParams {
  connectionId?: string;
  id: string;
  data: Record<string, unknown>;
}

/** Result of editing a row: the saved row, plus the outcome of any external write-back. */
export interface DatasetUpdateResult {
  row: Record<string, unknown>;
  /** `'queued'`: accepted locally, a debounced/scheduled sweep will submit it later — not a transport ack yet. */
  external?: { attempted: boolean; status?: 'sent' | 'error' | 'queued'; error?: string };
}

/**
 * A queryable table exposed by the satellite. Each provider registers itself
 * into DatasetRegistryService on bootstrap (mirrors OperationRegistry).
 */
export interface DatasetProvider {
  readonly descriptor: DatasetDescriptor;
  query(params: DatasetQuery): Promise<DatasetPage>;
  /** Optional: providers that opt into deletion (descriptor.deletable) implement this. */
  deleteRows?(params: DatasetDeleteParams): Promise<{ affected: number }>;
  /** Optional: providers that opt into editing (descriptor.editable) implement this. */
  update?(params: DatasetUpdateParams): Promise<DatasetUpdateResult>;
}

/**
 * A dynamic source of datasets whose descriptors are not known at boot time
 * (e.g. user-managed table templates stored in Mongo). The registry merges
 * these with statically registered providers.
 */
export interface DatasetSource {
  listDescriptors(): Promise<DatasetDescriptor[]>;
  resolve(key: string): Promise<DatasetProvider | null>;
}
