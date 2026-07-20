# Emisión outbound de "emitidas" (yalia_sii)

Cuando `submitGroup()` cierra un envío saliente, sii spoolea un evento de dominio
(`emitida.sent` / `emitida.error`) en la misma transacción que la transición de
estado de las filas, y el hub lo enruta —vía un flow— hasta un `yalia_emitter`
que hace el POST final a un sistema externo (en este piloto, un **mock**).

La emisión outbound es **local de sii**, en `src/outbox/`: un patrón autocontenido
y copiable, sin paquete compartido ni registry. Otro satélite (sftp, netsuite…)
que necesite emitir eventos copia esos ficheros y adapta el store a su
persistencia. Mismo patrón que el outbox propio de netsuite
(`yalia_netsuite/src/outbox/`).

## Qué es `hub-events`

Cola Redis compartida ya existente (`QUEUES.HUB_EVENTS`): el canal por el que un
**satélite empuja un evento hacia el hub**. El `HubEventsProcessor` del hub la
consume, resuelve qué flow arrancar (por `flowId` explícito → por origen satélite
`findBySatelliteOrigin(sourceKey, connectionId, operation)` → por `routeKey`) y
llama a `pipeline.startFlow`. La emisión de sii solo aporta un **productor fiable**
(outbox transaccional) a ese canal.

## Piezas en sii (`src/outbox/`)

- `domain-event.types.ts` → contrato `DomainEvent`, envelope `HubEventJob`,
  `OutboxRecord` y el puerto `OutboxStore` (lo que reimplementa quien copie el
  patrón). El `HubEventJob` debe seguir en sync con
  `yalia_hub/src/core/broker-ingress/broker-ingress.types.ts`.
- `domain-event-outbox.entity.ts` + migración `1752800000000-CreateDomainEventOutbox`
  → tabla `domain_event_outbox` (corre sola al arrancar, `migrationsRun:true`).
- `typeorm-outbox.store.ts` → adapter Postgres del puerto `OutboxStore`.
- `domain-emitter.service.ts` → `emit(event, tx?)`: solo spool (outbox puro).
- `outbox-drain.cron.ts` → `@Cron('*/5s')` publica lo pendiente en `hub-events`
  (`sourceKey:'sii'`), con reintentos y corte a dead-letter tras 5 intentos.
- `outbox.module.ts` → `@Module` plano: registra la entidad, la cola `hub-events`,
  el store, el service y el cron; exporta `DomainEmitterService`.
- `tables/table-rows.service.ts` → `commitOutcome()` ata `markGroupResult` + `emit`
  en una transacción, en los **4 desenlaces terminales** de `submitGroup` (2xx →
  `emitida.sent`; non-2xx, fallo de transporte y conexión-no-permitida →
  `emitida.error`). `recordRun()` (traza) sigue best-effort, fuera de la tx.
- `idempotencyKey = batchId`. Payload del evento: `{ tableKey, batchId,
  connectionId, connectionName, trigger, groupValues, rowCount, httpStatus,
  status, errorMessage, rowIds }`.

## Portar el patrón a otro satélite

1. Copiar `src/outbox/` (los 6 ficheros).
2. Reimplementar el store contra la BD del satélite (Postgres→otra tabla, o Mongo
   con Mongoose) manteniendo la interfaz `OutboxStore`; `save(event, tx?)` debe
   spoolear usando `tx` para ser atómico con la escritura de dominio.
3. Cambiar `SOURCE_KEY` en `outbox-drain.cron.ts` a la clave del satélite.
4. Importar `OutboxModule` donde ocurra la escritura de dominio e inyectar
   `DomainEmitterService`, emitiendo dentro de su transacción.

## Cableado del egress contra el mock (manual)

Objetivo del flow: `origin satellite(sii, emitida.sent) → adapter (JSONata) →
destination satellite(emitter, connId=mock)`. Todo vía las APIs de gestión
existentes (sin UI nueva; la config de hooks per-satélite en su front es una
iteración futura).

1. **Mock**: una URL que reciba el POST (p.ej. `https://webhook.site/<uuid>` o un
   echo local).

2. **Conexión en el emitter** — `POST /connections` (yalia_emitter): `baseUrl` =
   la URL del mock, sin auth. Anota el `connectionId` resultante → `MOCK_CONN`.

3. **Send-rule en el emitter** — `POST /v1/send-rules`: modo per-record, método
   `POST`. Anota su id → `SEND_RULE_ID`.

4. **Adapter (JSONata) en el hub** — `POST /definitions/adapters`: un `routeKey`
   nuevo (p.ej. `emitida_to_mock`) cuyo `mapping` transforme el payload del evento
   al body que espera el mock. Probar con `POST /definitions/adapters/test-inline`.

5. **Flow en el hub** — `POST /definitions/flows`:
   ```jsonc
   {
     "flowId": "sii_emitida_to_mock",
     "origins": [
       { "type": "satellite", "satelliteKey": "sii", "operation": "emitida.sent" }
       // opcional: "connectionId": "<conexión sii>" para acotar a una sola conexión.
       // Sin él, resuelve para cualquier conexión de origen de sii.
     ],
     "adapters": [{ "routeKey": "emitida_to_mock" }],
     "destinations": [
       { "type": "satellite", "satelliteKey": "emitter", "operation": "webhook.send",
         "connectionId": "MOCK_CONN", "params": { "sendRuleId": "SEND_RULE_ID" } }
     ]
   }
   ```
   Clave: la conexión del emitter (mock) va en `destination.connectionId` — NO es
   la conexión de origen de sii que viaja en el evento. Para `emitida.error`,
   crea un segundo flow análogo (o añade el `operation` como otro origin).

## Verificación end-to-end

1. **Build** (verificación real; `npm run build` es más estricto que lint) ✅.
2. Arrancar sii → confirmar que existe la tabla `domain_event_outbox`.
3. **Camino feliz**: disparar un envío. En la misma transacción: `table_rows`
   pasa a `pending` + fila spooleada en `domain_event_outbox`; en ~5s `drained_at`
   se marca y el job entra en `hub-events`. El hub resuelve el flow → emitter → el
   **mock recibe el POST**.
4. **Camino de error**: apuntar la conexión sii a un endpoint 4xx/5xx → se
   spoolea y entrega `emitida.error`.
5. **Resiliencia (no pérdida)**: parar el hub/Redis, disparar un envío → la fila
   del outbox queda pendiente (no se pierde) y drena al volver el servicio.

## Nota / edge conocido

Los 4 puntos de `commitOutcome` viven dentro del `try/catch` que rodea el envío
HTTP. Si la transacción (UPDATE + spool) fallara en el camino 2xx —un fallo de BD
raro—, el `catch` lo trataría como error de transporte: las filas vuelven a
`queued` (reenvío en la siguiente pasada) y se emitiría `emitida.error`. Es la
misma tolerancia que ya tenía el `markGroupResult` original (siempre estuvo dentro
del `try`); eventualmente consistente al reenviar.
