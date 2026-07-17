import {
  IsBoolean,
  IsIn,
  IsInt,
  IsObject,
  IsOptional,
  Matches,
  Max,
  Min,
  IsString,
  ValidateNested,
} from 'class-validator';
import { Type } from 'class-transformer';
import type {
  HandshakeConfig,
  HttpMethod,
  SourceAuthType,
  SourceCredentials,
} from '../entities/source-connection.entity';

const AUTH_TYPES: SourceAuthType[] = ['bearer'];
const HTTP_METHODS: HttpMethod[] = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD'];

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

  /** Alfanumérico en minúsculas + guiones; vacío = derivar de `name`. */
  @IsString()
  @IsOptional()
  @Matches(/^[a-z0-9-]+$/, { message: 'clave solo admite minúsculas, números y guiones' })
  clave?: string;

  /** Cadencia (segundos) del cron interno de envío para esta conexión. 0/omitido = sin cron. */
  @IsInt()
  @Min(0)
  @IsOptional()
  writeCronIntervalSec?: number;

  /** Nº de lotes enviados en paralelo por "Forzar envío"/barrido. 1/omitido = secuencial. */
  @IsInt()
  @Min(1)
  @Max(50)
  @IsOptional()
  concurrency?: number;

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
  @Type(() => HandshakeDto)
  @IsOptional()
  handshake?: HandshakeDto;

  @IsBoolean()
  @IsOptional()
  active?: boolean;
}
