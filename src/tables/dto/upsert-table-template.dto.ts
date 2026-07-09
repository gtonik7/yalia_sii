import {
  ArrayMinSize,
  IsArray,
  IsBoolean,
  IsIn,
  IsInt,
  IsObject,
  IsOptional,
  IsString,
  Matches,
  Min,
  MinLength,
  ValidateNested,
} from 'class-validator';
import { Type } from 'class-transformer';

const COLUMN_TYPES = ['string', 'number', 'date', 'boolean', 'json'] as const;

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
}

export class TableSortDto {
  @IsString()
  key!: string;

  @IsIn(['asc', 'desc'])
  dir!: 'asc' | 'desc';
}

export class AuditIncrementalDto {
  @IsString()
  updatedAtField!: string;

  @IsString()
  sinceParam!: string;

  @IsOptional()
  @IsIn(['query', 'body'])
  sinceIn?: 'query' | 'body';

  @IsOptional()
  @IsIn(['iso', 'epoch_ms', 'epoch_s'])
  sinceFormat?: 'iso' | 'epoch_ms' | 'epoch_s';
}

export class AuditConfigDto {
  @IsString()
  @MinLength(1)
  connectionId!: string;

  @IsIn(['GET', 'POST'])
  method!: 'GET' | 'POST';

  @IsString()
  @MinLength(1)
  path!: string;

  @IsOptional()
  @IsObject()
  query?: Record<string, string>;

  @IsOptional()
  @IsObject()
  body?: Record<string, unknown>;

  @IsOptional()
  @IsString()
  recordsPath?: string;

  @IsOptional()
  @ValidateNested()
  @Type(() => AuditIncrementalDto)
  incremental?: AuditIncrementalDto;
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
}

export class WriteConfigDto {
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

  @IsOptional()
  @IsString()
  externalRefPath?: string;

  @IsIn(['event', 'schedule'])
  trigger!: 'event' | 'schedule';

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
  @IsBoolean()
  perConnection?: boolean;

  /** When perConnection, the source-connection ids this table is exposed on. */
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  connectionIds?: string[];

  @IsOptional()
  @IsString()
  idField?: string;

  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => TableColumnDto)
  columns!: TableColumnDto[];

  @IsOptional()
  @ValidateNested()
  @Type(() => TableSortDto)
  defaultSort?: TableSortDto;

  /** Present to make this table pull/audit an external source by paging it. */
  @IsOptional()
  @ValidateNested()
  @Type(() => AuditConfigDto)
  audit?: AuditConfigDto;

  /** Present to push edited rows back to an external source. */
  @IsOptional()
  @ValidateNested()
  @Type(() => WriteConfigDto)
  write?: WriteConfigDto;
}
