import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import parser from 'cron-parser';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { BackupSchedule } from './entities/backup-schedule.entity';
import { BackupService } from './backup.service';

/**
 * Supervisor `setInterval` que ejecuta los backups programados (mismo patrón que
 * `TableRetentionCron`/`WriteCronService`: sin Redis, in-process, `timer.unref()`,
 * guard `tickInProgress`). En cada tick evalúa cada schedule habilitado: si el
 * slot cron previo es posterior al `lastRunAt`, hay un backup pendiente y se
 * lanza. Esto tolera ticks gruesos y reinicios (se ejecuta el slot pendiente una
 * sola vez, sin acumular ejecuciones perdidas).
 */
@Injectable()
export class BackupCron implements OnModuleInit, OnModuleDestroy {
    private readonly logger = new Logger(BackupCron.name);
    private readonly tickMs = Number(process.env.BACKUP_TICK_MS) || 60_000;
    private timer: NodeJS.Timeout | null = null;
    private tickInProgress = false;

    constructor(
        @InjectRepository(BackupSchedule) private readonly schedules: Repository<BackupSchedule>,
        private readonly backup: BackupService,
    ) {}

    onModuleInit(): void {
        this.timer = setInterval(() => void this.tick(), this.tickMs);
        this.timer.unref?.();
        this.logger.log(`Backup cron iniciado (tick ${this.tickMs}ms)`);
    }

    onModuleDestroy(): void {
        if (this.timer) clearInterval(this.timer);
        this.timer = null;
    }

    private async tick(): Promise<void> {
        if (this.tickInProgress) return;
        this.tickInProgress = true;
        try {
            const now = new Date();
            const schedules = await this.schedules.find({ where: { enabled: true } });
            for (const s of schedules) {
                if (!this.isDue(s.cronExpression, s.lastRunAt, now)) continue;
                try {
                    await this.backup.runBackup({
                        scheduleId: s.id,
                        trigger: 'schedule',
                        tables: s.tables ?? [],
                        destinations: s.destinations ?? {},
                        retentionCount: s.retentionCount,
                    });
                } catch (err) {
                    this.logger.warn(`Backup programado falló schedule=${s.id}: ${err instanceof Error ? err.message : String(err)}`);
                }
            }
        } catch (err) {
            this.logger.error(`Backup cron tick falló: ${err instanceof Error ? err.message : String(err)}`);
        } finally {
            this.tickInProgress = false;
        }
    }

    /** ¿Hay un slot cron previo posterior al último run? */
    private isDue(expr: string, lastRunAt: Date | null, now: Date): boolean {
        try {
            const it = parser.parseExpression(expr, { currentDate: now });
            const prev = it.prev().toDate();
            return !lastRunAt || lastRunAt.getTime() < prev.getTime();
        } catch {
            return false;
        }
    }
}
