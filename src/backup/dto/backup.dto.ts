import { IsArray, IsBoolean, IsIn, IsInt, IsObject, IsOptional, IsString, Min } from 'class-validator';
import type { BackupDestinations } from '../entities/backup-schedule.entity';

export class UpsertBackupScheduleDto {
    @IsString()
    name!: string;

    @IsArray()
    @IsString({ each: true })
    tables!: string[];

    @IsString()
    cronExpression!: string;

    // Objeto anidado libre: `{ local?, download?, email?: { to: string[] } }`.
    @IsObject()
    destinations!: BackupDestinations;

    @IsOptional()
    @IsInt()
    @Min(0)
    retentionCount?: number;

    @IsOptional()
    @IsBoolean()
    enabled?: boolean;
}

export class RunBackupDto {
    @IsOptional()
    @IsArray()
    @IsString({ each: true })
    tables?: string[];

    @IsObject()
    destinations!: BackupDestinations;
}

export class RestoreDto {
    @IsOptional()
    @IsString()
    runId?: string;

    @IsOptional()
    @IsString()
    uploadBase64?: string;

    @IsOptional()
    @IsArray()
    @IsString({ each: true })
    tables?: string[];

    @IsIn(['replace', 'append'])
    mode!: 'replace' | 'append';

    @IsBoolean()
    confirm!: boolean;
}
