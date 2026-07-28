import { Body, Controller, Delete, Get, HttpCode, Param, Post, Put, Res, UseGuards } from '@nestjs/common';
import type { FastifyReply } from 'fastify';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { BackupService } from './backup.service';
import { RestoreService } from './restore.service';
import { RestoreDto, RunBackupDto, UpsertBackupScheduleDto } from './dto/backup.dto';

@Controller('v1/backups')
@UseGuards(MgmtTokenGuard)
export class BackupController {
    constructor(
        private readonly backup: BackupService,
        private readonly restore: RestoreService,
    ) {}

    // ── Tablas backupeables ───────────────────────────────────────────────────
    @Get('tables')
    tables() {
        return this.backup.listTables();
    }

    // ── Schedules ──────────────────────────────────────────────────────────────
    @Get('schedules')
    listSchedules() {
        return this.backup.listSchedules();
    }

    @Post('schedules')
    createSchedule(@Body() dto: UpsertBackupScheduleDto) {
        return this.backup.createSchedule(dto);
    }

    @Put('schedules/:id')
    updateSchedule(@Param('id') id: string, @Body() dto: UpsertBackupScheduleDto) {
        return this.backup.updateSchedule(id, dto);
    }

    @Delete('schedules/:id')
    async deleteSchedule(@Param('id') id: string) {
        await this.backup.deleteSchedule(id);
        return { deleted: true };
    }

    @Post('schedules/:id/trigger')
    async trigger(@Param('id') id: string) {
        const schedule = await this.backup.getSchedule(id);
        // async: crea el run 'running' y encola el pg_dump; devuelve el run al instante.
        return this.backup.enqueueBackup({
            scheduleId: schedule.id,
            trigger: 'manual',
            tables: schedule.tables ?? [],
            destinations: schedule.destinations ?? {},
            retentionCount: schedule.retentionCount,
        });
    }

    // ── Backup ad-hoc ("ahora") ────────────────────────────────────────────────
    @Post('run')
    runNow(@Body() dto: RunBackupDto) {
        return this.backup.enqueueBackup({
            trigger: 'manual',
            tables: dto.tables ?? [],
            destinations: dto.destinations ?? {},
        });
    }

    // ── Historial de runs ──────────────────────────────────────────────────────
    @Get('runs')
    listRuns() {
        return this.backup.listRuns();
    }

    @Delete('runs/:id')
    async deleteRun(@Param('id') id: string) {
        await this.backup.deleteRun(id);
        return { deleted: true };
    }

    @Get('runs/:id/download')
    async download(@Param('id') id: string, @Res() reply: FastifyReply) {
        const { stream, fileName, sizeBytes } = await this.backup.getArtifact(id);
        reply.header('Content-Type', 'application/octet-stream');
        reply.header('Content-Disposition', `attachment; filename="${fileName}"`);
        if (sizeBytes) reply.header('Content-Length', String(sizeBytes));
        return reply.send(stream);
    }

    // ── Restauración (destructiva) ─────────────────────────────────────────────
    @Post('restore')
    @HttpCode(202)
    async restoreBackup(@Body() dto: RestoreDto) {
        // async: valida + prepara fichero, crea operation_run y encola pg_restore.
        const run = await this.restore.enqueueRestore(dto);
        return { runId: run.id };
    }
}
