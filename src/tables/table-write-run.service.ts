import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { TableWriteRun } from './entities/table-write-run.entity';

/**
 * Records the history of outbound submission batches. Unlike the poll-run
 * lifecycle (create → progress → complete), a write run is a single terminal
 * fact recorded once, after the awaited `submitGroup()` HTTP call resolves —
 * there is no in-flight "running" phase worth exposing for a one-shot batch.
 */
@Injectable()
export class TableWriteRunService {
  constructor(
    @InjectRepository(TableWriteRun)
    private readonly repo: Repository<TableWriteRun>,
  ) {}

  record(data: {
    tableKey: string;
    connectionId?: string | null;
    connectionName?: string | null;
    trigger: 'event' | 'schedule' | 'manual';
    status: 'sent' | 'error';
    batchId?: string | null;
    groupValues?: Record<string, string> | null;
    rowCount: number;
    httpStatus?: number | null;
    errorMessage?: string | null;
  }): Promise<TableWriteRun> {
    return this.repo.save(this.repo.create({ ...data, completedAt: new Date() }));
  }
}
