import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { SourcePollRun } from './entities/source-poll-run.entity';

@Injectable()
export class SourcePollRunService {
  constructor(
    @InjectRepository(SourcePollRun)
    private readonly repo: Repository<SourcePollRun>,
  ) {}

  create(data: {
    tableKey: string;
    connectionId: string;
    connectionName?: string | null;
    trigger: 'manual' | 'scheduled';
    since: string | null;
  }): Promise<SourcePollRun> {
    return this.repo.save(this.repo.create({ ...data, status: 'running' }));
  }

  /** Live progress update while the run is in flight. */
  async progress(
    runId: string,
    patch: { pages?: number; fetched?: number; inserted?: number; upserted?: number },
  ): Promise<void> {
    await this.repo.update({ id: runId }, patch);
  }

  async complete(
    runId: string,
    totals: { pages: number; fetched: number; inserted: number; upserted: number },
  ): Promise<void> {
    const status = totals.fetched === 0 ? 'empty' : 'completed';
    await this.repo.update({ id: runId }, { ...totals, status, completedAt: new Date() });
  }

  async fail(runId: string, errorMessage: string): Promise<void> {
    await this.repo.update({ id: runId }, { status: 'error', errorMessage, completedAt: new Date() });
  }

  listByTable(tableKey: string | undefined, limit = 20): Promise<SourcePollRun[]> {
    return this.repo.find({
      where: tableKey ? { tableKey } : {},
      order: { createdAt: 'DESC' },
      take: limit,
    });
  }

  async findById(runId: string): Promise<SourcePollRun> {
    const doc = await this.repo.findOne({ where: { id: runId } });
    if (!doc) throw new NotFoundException(`Poll run "${runId}" not found`);
    return doc;
  }
}
