import {
  IsBoolean,
  IsIn,
  IsObject,
  IsOptional,
  IsString,
  ValidateNested,
} from 'class-validator';
import { Type } from 'class-transformer';
import type {
  HandshakeConfig,
  HttpMethod,
  PaginationConfig,
  PaginationType,
  SourceAuthType,
  SourceCredentials,
} from '../entities/source-connection.entity';

const AUTH_TYPES: SourceAuthType[] = ['bearer'];
const PAGINATION_TYPES: PaginationType[] = ['none', 'page', 'offset', 'cursor', 'link', 'nextUrl'];
const HTTP_METHODS: HttpMethod[] = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD'];

class PaginationDto implements PaginationConfig {
  @IsIn(PAGINATION_TYPES)
  type!: PaginationType;

  @IsString()
  recordsPath!: string;

  @IsString() @IsOptional() pageParam?: string;
  @IsString() @IsOptional() pageSizeParam?: string;
  @IsOptional() pageSize?: number;
  @IsOptional() startPage?: number;
  @IsString() @IsOptional() offsetParam?: string;
  @IsString() @IsOptional() limitParam?: string;
  @IsString() @IsOptional() isLastPath?: string;
  @IsString() @IsOptional() totalPagesPath?: string;
  @IsString() @IsOptional() totalResultsPath?: string;
  @IsString() @IsOptional() cursorParam?: string;
  @IsString() @IsOptional() nextCursorPath?: string;
  @IsString() @IsOptional() nextUrlPath?: string;
}

class HandshakeDto implements HandshakeConfig {
  @IsIn(HTTP_METHODS)
  method!: HttpMethod;

  @IsString() @IsOptional() path?: string;
  @IsObject() @IsOptional() query?: Record<string, string>;
  @IsOptional() body?: unknown;
}

export class UpsertSourceConnectionDto {
  @IsString()
  name!: string;

  @IsString()
  baseUrl!: string;

  @IsIn(AUTH_TYPES)
  authType!: SourceAuthType;

  /** Plaintext credentials; encrypted before persisting. Optional on update. */
  @IsObject()
  @IsOptional()
  credentials?: SourceCredentials;

  @IsObject()
  @IsOptional()
  defaultHeaders?: Record<string, string>;

  @ValidateNested()
  @Type(() => PaginationDto)
  pagination!: PaginationDto;

  @ValidateNested()
  @Type(() => HandshakeDto)
  @IsOptional()
  handshake?: HandshakeDto;

  @IsBoolean()
  @IsOptional()
  active?: boolean;
}
