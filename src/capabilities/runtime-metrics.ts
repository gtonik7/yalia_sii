import * as os from 'node:os';
import { statfs } from 'node:fs/promises';

/**
 * Métricas del host y del proceso Node del satélite. Se recogen en-proceso, sin
 * dependencias externas. Copia del helper homónimo del hub (patrón replicado por
 * satélite, sin paquete compartido). Bytes crudos: el FE los formatea.
 */
export interface HostMetrics {
  /** Utilización de CPU del host en [0..1] (1 = 100%). `null` si no se pudo muestrear. */
  cpuUsage: number | null;
  /** Load average [1m, 5m, 15m]. En Windows devuelve [0,0,0]; válido en Linux. */
  loadAvg: [number, number, number];
  memTotalBytes: number;
  memFreeBytes: number;
  /** Espacio del filesystem donde corre el proceso. `null` si statfs falla. */
  diskTotalBytes: number | null;
  diskFreeBytes: number | null;
  processRssBytes: number;
  processHeapUsedBytes: number;
  processHeapTotalBytes: number;
  hostUptimeSeconds: number;
}

interface CpuTimesTotals {
  idle: number;
  total: number;
}

function cpuTotals(): CpuTimesTotals {
  let idle = 0;
  let total = 0;
  for (const cpu of os.cpus()) {
    const t = cpu.times;
    idle += t.idle;
    total += t.user + t.nice + t.sys + t.idle + t.irq;
  }
  return { idle, total };
}

/** Utilización de CPU del host muestreando `os.cpus()` dos veces con un gap corto. */
async function sampleCpuUsage(gapMs = 150): Promise<number | null> {
  try {
    const a = cpuTotals();
    await new Promise((r) => setTimeout(r, gapMs));
    const b = cpuTotals();
    const idleDelta = b.idle - a.idle;
    const totalDelta = b.total - a.total;
    if (totalDelta <= 0) return null;
    const usage = 1 - idleDelta / totalDelta;
    return Math.min(1, Math.max(0, usage));
  } catch {
    return null;
  }
}

async function diskBytes(): Promise<{ total: number | null; free: number | null }> {
  try {
    const s = await statfs(process.cwd());
    return { total: s.blocks * s.bsize, free: s.bavail * s.bsize };
  } catch {
    return { total: null, free: null };
  }
}

export async function collectHostMetrics(): Promise<HostMetrics> {
  const [cpuUsage, disk] = await Promise.all([sampleCpuUsage(), diskBytes()]);
  const load = os.loadavg();
  const mem = process.memoryUsage();
  return {
    cpuUsage,
    loadAvg: [load[0] ?? 0, load[1] ?? 0, load[2] ?? 0],
    memTotalBytes: os.totalmem(),
    memFreeBytes: os.freemem(),
    diskTotalBytes: disk.total,
    diskFreeBytes: disk.free,
    processRssBytes: mem.rss,
    processHeapUsedBytes: mem.heapUsed,
    processHeapTotalBytes: mem.heapTotal,
    hostUptimeSeconds: Math.round(os.uptime()),
  };
}
