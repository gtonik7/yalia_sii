import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { BullModule } from '@nestjs/bullmq';
import { BackupSchedule } from './entities/backup-schedule.entity';
import { BackupRun } from './entities/backup-run.entity';
import { OperationRun } from '../tables/entities/operation-run.entity';
import { BackupService } from './backup.service';
import { RestoreService } from './restore.service';
import { MailService } from './mail.service';
import { BackupCron } from './backup.cron';
import { BackupController } from './backup.controller';
import { BackupProcessor } from './backup.processor';
import { QUEUES } from '../core/queues/queues.constants';

/**
 * Backup/restore programado de la BD de yalia_sii: `pg_dump -Fc` por tablas
 * seleccionadas, con destinos local/descarga/email, y restore `pg_restore
 * --data-only`. Cron in-process (`BackupCron`, patrón `TableRetentionCron`).
 * pg_dump/pg_restore corren en la cola BACKUP (`BackupProcessor`) para no bloquear
 * el request HTTP; el restore registra su estado en `operation_runs` (entidad genérica).
 */
@Module({
    imports: [TypeOrmModule.forFeature([BackupSchedule, BackupRun, OperationRun]), BullModule.registerQueue({ name: QUEUES.BACKUP })],
    controllers: [BackupController],
    providers: [BackupService, RestoreService, MailService, BackupCron, BackupProcessor],
})
export class BackupModule {}
