/**
 * Emisión outbound de eventos de dominio (local de sii).
 *
 * Cuando ocurre un hecho de dominio en el satélite (una "emitida" enviada/fallida),
 * `TableRowsService` construye un {@link DomainEvent} y lo entrega a
 * `DomainEmitterService.emit()`, que lo spoolea en el outbox `domain_event_outbox`
 * dentro de la MISMA transacción que la escritura de dominio. El `OutboxDrainCron`
 * lo publica luego en la cola compartida `hub-events` como {@link HubEventJob}, que
 * el hub consume y enruta a un flow.
 *
 * Este es un patrón autocontenido y copiable: otro satélite (sftp, netsuite…) que
 * necesite emitir eventos copia estos ficheros de `src/outbox/` y adapta el store
 * a su persistencia (aquí Postgres/TypeORM; en Mongo sería un adapter Mongoose).
 */
export interface DomainEvent {
  /**
   * Discriminador del hecho de dominio. Es lo que, junto al `sourceKey` del
   * satélite, resuelve el flow en el hub (`findBySatelliteOrigin`).
   * Ej.: `'emitida.sent' | 'emitida.error'`.
   */
  operation: string;
  /** Cuerpo del evento; el adapter (JSONata) del flow lo reamolda al destino. */
  payload: unknown;
  /** Conexión de origen bajo la que ocurrió el hecho (usada para resolver el flow). */
  connectionId: string;
  traceId?: string;
  /** Clave de idempotencia (p.ej. el batchId del envío) para no reprocesar en replays. */
  idempotencyKey?: string;
  /** Atajo opcional: fija el flow por id en lugar de resolver por (sourceKey, operation). */
  flowId?: string;
  routeKey?: string;
}

/**
 * Envelope que viaja por la cola `hub-events`. DEBE mantenerse en sync con
 * `yalia_hub/src/core/broker-ingress/broker-ingress.types.ts` (HubEventJob), que
 * es quien lo consume en `hub-events.processor.ts`.
 */
export interface HubEventJob {
  sourceKey: string;
  routeKey?: string;
  flowId?: string;
  operation?: string;
  payload: unknown;
  connectionId: string;
  idempotencyKey?: string;
  traceId?: string;
}

/** Un evento spooleado, tal como lo devuelve el store al drenar. */
export interface OutboxRecord {
  id: string;
  event: DomainEvent;
  attempts: number;
}

/**
 * Contrato de persistencia del outbox. `TypeOrmOutboxStore` lo implementa sobre
 * Postgres. Al portar el patrón a otro satélite, esta interfaz es lo que hay que
 * reimplementar contra su BD; el service y el cron no dependen de la persistencia.
 */
export interface OutboxStore {
  /**
   * Persiste un evento pendiente de publicar. `tx` es un handle de transacción
   * opaco (aquí el `EntityManager` de TypeORM); cuando se pasa, el spool es atómico
   * con la escritura de dominio del llamante.
   */
  save(event: DomainEvent, tx?: unknown): Promise<void>;
  /** Eventos aún no drenados ni descartados, en orden de llegada. */
  getPending(limit: number): Promise<OutboxRecord[]>;
  markDrained(id: string): Promise<void>;
  recordAttempt(id: string, error: string): Promise<void>;
  markDeadLetter(id: string): Promise<void>;
}
