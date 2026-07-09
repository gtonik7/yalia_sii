import { WriteCronService } from './write-cron.service';
import type { TableTemplate } from './entities/table-template.entity';

/** Let the fire-and-forget sweepConnection() dispatched inside supervisorTick() settle. */
const flush = () => new Promise((r) => setTimeout(r, 5));

function tpl(over: Partial<TableTemplate>): TableTemplate {
  return { key: 'k', label: 'k', write: null, ...over } as TableTemplate;
}

const writeCfg = { connectionId: 'x', method: 'POST' as const, path: '/', trigger: 'schedule' as const };

/**
 * Pure unit spec — no DB, no Redis. The internal cron's whole job is: pick the
 * connections whose interval elapsed, and sweep each write-enabled table
 * through TableWriteBatchService for that connection. We drive supervisorTick()
 * directly (via bracket access) so there's no timer flakiness.
 */
describe('WriteCronService — internal per-connection write cron', () => {
  function build(opts: { conns?: { id: string; intervalSec: number }[]; templates?: TableTemplate[]; submit?: jest.Mock }) {
    const connections = { listWriteCronConnections: jest.fn().mockResolvedValue(opts.conns ?? []) };
    const templates = { findAll: jest.fn().mockResolvedValue(opts.templates ?? []) };
    const submit = opts.submit ?? jest.fn().mockResolvedValue(undefined);
    const writeBatch = { submitAllQueued: submit };
    const service = new WriteCronService(connections as never, templates as never, writeBatch as never);
    return { service, connections, templates, submit };
  }

  it('sweeps a due connection once per schedule-mode table (skipping no-write and event-mode templates)', async () => {
    const submit = jest.fn().mockResolvedValue(undefined);
    const templates = [
      tpl({ key: 'emitidas', write: writeCfg }),
      tpl({ key: 'no-write', write: null }),
      tpl({ key: 'event-mode', write: { ...writeCfg, trigger: 'event' } }),
    ];
    const { service } = build({ conns: [{ id: 'c1', intervalSec: 10 }], templates, submit });

    await service['supervisorTick']();
    await flush();

    // Only the schedule-mode template is swept; no-write and event-mode are skipped.
    expect(submit).toHaveBeenCalledTimes(1);
    expect(submit).toHaveBeenCalledWith(templates[0], 'schedule', 'c1');
  });

  it('does not sweep again before the interval has elapsed', async () => {
    const submit = jest.fn().mockResolvedValue(undefined);
    const templates = [tpl({ key: 'emitidas', write: writeCfg })];
    const { service } = build({ conns: [{ id: 'c1', intervalSec: 3600 }], templates, submit });

    await service['supervisorTick'](); // first time → due → sweeps
    await flush();
    await service['supervisorTick'](); // immediately again → not due
    await flush();

    expect(submit).toHaveBeenCalledTimes(1);
  });

  it('skips a connection whose previous sweep is still in flight (overlap guard)', async () => {
    const submit = jest.fn().mockResolvedValue(undefined);
    const templates = [tpl({ key: 'emitidas', write: writeCfg })];
    const { service } = build({ conns: [{ id: 'c1', intervalSec: 1 }], templates, submit });
    service['running'].add('c1'); // simulate an in-flight sweep

    await service['supervisorTick']();
    await flush();

    expect(submit).not.toHaveBeenCalled();
  });

  it('continues to other templates when one template sweep throws', async () => {
    const submit = jest.fn().mockRejectedValueOnce(new Error('boom')).mockResolvedValueOnce(undefined);
    const templates = [
      tpl({ key: 't1', write: writeCfg }),
      tpl({ key: 't2', write: writeCfg }),
    ];
    const { service } = build({ conns: [{ id: 'c1', intervalSec: 10 }], templates, submit });

    await service['supervisorTick']();
    await flush();

    expect(submit).toHaveBeenCalledTimes(2); // second still attempted despite the first throwing
  });
});
