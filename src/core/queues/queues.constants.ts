/**
 * `@Processor(QUEUES.JOBS)`, `BullModule.registerQueue({ name: QUEUES.JOBS })` etc.
 * resuelven el nombre en carga de módulo — antes de que ConfigModule parsee el .env.
 * Por eso se lee `process.env.SATELLITE_KEY` directo aquí; `config/env.ts` valida formalmente.
 */
export function satelliteKey(): string {
  return process.env.SATELLITE_KEY ?? 'datatable';
}

export function satelliteJobsQueueName(key: string = satelliteKey()): string {
  return `sat-${key}-jobs`;
}

/** Immediate, row-targeted send of a single edited row (event mode) as an array of 1. */
export function satelliteWriteEventQueueName(key: string = satelliteKey()): string {
  return `sat-${key}-write-event`;
}

/** Control channel (hub → satélite): provisión/rotación del token de gestión. */
export function satelliteControlQueueName(key: string = satelliteKey()): string {
  return `sat-${key}-control`;
}

export const QUEUES = {
  JOBS: satelliteJobsQueueName(),
  WRITE_EVENT: satelliteWriteEventQueueName(),
  CONTROL: satelliteControlQueueName(),
  // Inbound SII-result callback: fixed name (not per-satellite-key) since this
  // satellite's identity is permanently "sii" — mirrors the HUB_* queues below.
  SII_INBOUND: 'sii-inbound-results',
  HUB_CALLBACKS: 'hub-callbacks',
  HUB_EVENTS: 'hub-events',
  HUB_ANNOUNCE: 'hub-announce',
} as const;

export type QueueName = (typeof QUEUES)[keyof typeof QUEUES];

export const DEFAULT_JOB_OPTS = {
  attempts: 5,
  backoff: { type: 'exponential' as const, delay: 2000 },
  removeOnComplete: { age: 3600, count: 1000 },
  removeOnFail: true,
};
