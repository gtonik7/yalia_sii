import { ArrayMinSize, IsArray, IsBoolean, IsIn, IsInt, IsObject, IsOptional, IsString, Matches, Min, MinLength, ValidateNested } from 'class-validator';
import { Type } from 'class-transformer';

const COLUMN_TYPES = ['string', 'number', 'date', 'boolean', 'json'] as const;

export class NumberFormatDto {
    @IsOptional()
    @IsBoolean()
    raw?: boolean;

    @IsOptional()
    @IsInt()
    @Min(0)
    decimals?: number;

    @IsOptional()
    @IsString()
    separator?: string;

    @IsOptional()
    @IsString()
    decimalSeparator?: string;

    @IsOptional()
    @IsString()
    prefix?: string;

    @IsOptional()
    @IsString()
    suffix?: string;
}

export class DateFormatDto {
    @IsOptional()
    @IsBoolean()
    raw?: boolean;

    @IsOptional()
    @IsString()
    pattern?: string;
}

export class TableColumnDto {
    @IsString()
    @Matches(/^[A-Za-z0-9_.-]+$/, { message: 'column key: letters, digits, _ . -' })
    key!: string;

    @IsString()
    @MinLength(1)
    label!: string;

    @IsIn(COLUMN_TYPES)
    type!: (typeof COLUMN_TYPES)[number];

    @IsOptional()
    @IsBoolean()
    filterable?: boolean;

    @IsOptional()
    @IsBoolean()
    sortable?: boolean;

    @IsOptional()
    @IsBoolean()
    hidden?: boolean;

    @IsOptional()
    @IsBoolean()
    readOnly?: boolean;

    @IsOptional()
    @IsBoolean()
    excludeFromPayload?: boolean;

    @IsOptional()
    @ValidateNested()
    @Type(() => NumberFormatDto)
    numberFormat?: NumberFormatDto;

    @IsOptional()
    @ValidateNested()
    @Type(() => DateFormatDto)
    dateFormat?: DateFormatDto;
}

export class TableSortDto {
    @IsString()
    key!: string;

    @IsIn(['asc', 'desc'])
    dir!: 'asc' | 'desc';
}

export class CollapseConfigDto {
    @IsOptional()
    @IsString()
    @MinLength(1)
    rowsField?: string;
}

export class BatchConfigDto {
    @IsArray()
    @ArrayMinSize(1)
    @IsString({ each: true })
    groupBy!: string[];

    @IsOptional()
    @IsInt()
    @Min(1)
    maxBatchSize?: number;

    @IsOptional()
    @IsInt()
    @Min(1)
    maxRecordsPerPoll?: number;

    @IsOptional()
    @ValidateNested()
    @Type(() => CollapseConfigDto)
    collapse?: CollapseConfigDto;
}

export class WriteConnectionRuleDto {
    /** Absent = generic/fallback rule (applies to any connection with no specific rule of its own). */
    @IsOptional()
    @IsString()
    @MinLength(1)
    connectionId?: string;

    @IsIn(['PUT', 'PATCH', 'POST'])
    method!: 'PUT' | 'PATCH' | 'POST';

    @IsString()
    @MinLength(1)
    path!: string;

    @IsOptional()
    @IsObject()
    query?: Record<string, string>;

    /** Override for edited rows (submission_status='revisado'); absent = reuse `method`. */
    @IsOptional()
    @IsIn(['PUT', 'PATCH', 'POST'])
    updateMethod?: 'PUT' | 'PATCH' | 'POST';

    /** Override for edited rows; absent = reuse `path`. */
    @IsOptional()
    @IsString()
    @MinLength(1)
    updatePath?: string;

    /** Override for edited rows; absent = reuse `query`. */
    @IsOptional()
    @IsObject()
    updateQuery?: Record<string, string>;

    /** Override for delete propagation (phase='delete'); absent = default 'DELETE'. */
    @IsOptional()
    @IsIn(['PUT', 'PATCH', 'POST', 'DELETE'])
    deleteMethod?: 'PUT' | 'PATCH' | 'POST' | 'DELETE';

    /** Override for delete propagation; absent = reuse `path`. */
    @IsOptional()
    @IsString()
    @MinLength(1)
    deletePath?: string;

    /** Override for delete propagation; absent = reuse `query`. */
    @IsOptional()
    @IsObject()
    deleteQuery?: Record<string, string>;
}

export class WriteConfigDto {
    /** true = the connection's write cron sweeps this table; false = only "Forzar envío". */
    @IsBoolean()
    scheduled!: boolean;

    /** true = rows are editable in the explorer; false = read-only detail view, but sending still applies. */
    @IsBoolean()
    editable!: boolean;

    /** true = the explorer exposes "Nuevo registro" for manual creation; independent of `editable`. */
    @IsOptional()
    @IsBoolean()
    creatable?: boolean;

    /** true = a local delete also propagates a DELETE to the external system for each affected row (best-effort). */
    @IsOptional()
    @IsBoolean()
    deleteEnabled?: boolean;

    /** false = hide the "Estado SII" column/field and every reference to it in the explorer and edit form; default/absent = true. */
    @IsOptional()
    @IsBoolean()
    showSiiStatus?: boolean;

    @IsArray()
    @ArrayMinSize(1)
    @ValidateNested({ each: true })
    @Type(() => WriteConnectionRuleDto)
    connections!: WriteConnectionRuleDto[];

    @IsOptional()
    @ValidateNested()
    @Type(() => BatchConfigDto)
    batch?: BatchConfigDto;
}

export class UpsertTableTemplateDto {
    @IsString()
    @Matches(/^[a-z0-9-]+$/, { message: 'key: lowercase letters, digits and hyphens only' })
    key!: string;

    @IsString()
    @MinLength(1)
    label!: string;

    @IsOptional()
    @IsString()
    description?: string;

    @IsOptional()
    @IsString()
    idField?: string;

    /** Composite upsert key (2+ column keys); mutually exclusive with `idField`. */
    @IsOptional()
    @IsArray()
    @IsString({ each: true })
    idFields?: string[];

    /** Column whose greatest numeric value decides which duplicate id wins ingest ("newest wins"). Requires idField. */
    @IsOptional()
    @IsString()
    recencyField?: string;

    @IsArray()
    @ValidateNested({ each: true })
    @Type(() => TableColumnDto)
    columns!: TableColumnDto[];

    @IsOptional()
    @ValidateNested()
    @Type(() => TableSortDto)
    defaultSort?: TableSortDto;

    /** Present to push edited rows back to an external source. */
    @IsOptional()
    @ValidateNested()
    @Type(() => WriteConfigDto)
    write?: WriteConfigDto;

    /** Opt-in automatic purge (days); unset = keep rows indefinitely. */
    @IsOptional()
    @IsInt()
    @Min(1)
    retentionDays?: number;

    /** Gate for the mass delete-by-filter operation (table.bulkDelete); default false. */
    @IsOptional()
    @IsBoolean()
    allowBulkDelete?: boolean;
}
