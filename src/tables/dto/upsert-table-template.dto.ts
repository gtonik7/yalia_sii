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
}

export class WriteConnectionRuleDto {
    @IsString()
    @MinLength(1)
    connectionId!: string;

    @IsIn(['PUT', 'PATCH', 'POST'])
    method!: 'PUT' | 'PATCH' | 'POST';

    @IsString()
    @MinLength(1)
    path!: string;

    @IsOptional()
    @IsObject()
    query?: Record<string, string>;
}

export class WriteConfigDto {
    @IsIn(['event', 'schedule'])
    trigger!: 'event' | 'schedule';

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

    /** The source-connection ids this table is exposed on. Empty/omitted = all connections. */
    @IsOptional()
    @IsArray()
    @IsString({ each: true })
    connectionIds?: string[];

    @IsOptional()
    @IsString()
    idField?: string;

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
