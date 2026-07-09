# Runbook: fila atascada en `submission_status = 'pending'`

Parte del pipeline de presentación SII/AEAT (`emitidas` y cualquier otra
template con `write` configurado). Ver también `src/tables/table-rows.service.ts`
(`submitGroup`), `src/tables/write-sweep.processor.ts` (modo evento) y
`src/tables/table-write-batch.service.ts` (modo schedule / red de seguridad).

## Qué significa "atascada en pending"

Una fila pasa a `submission_status = 'pending'` en cuanto `submitGroup()`
recibe un ACK 2xx del sistema externo — **no** significa que AEAT ya haya
resuelto la presentación. El resultado real solo llega después, vía el
callback entrante (`src/callbacks/aeat-result.processor.ts`), correlacionado
por `external_ref`.

Una fila queda "atascada" cuando lleva en `pending` más tiempo del que
tardaría el sistema externo en resolver una presentación en circunstancias
normales, sin que haya llegado ningún callback. Causas típicas:

1. El callback nunca llegó (el vendor no lo envió, la URL pública no es
   alcanzable, o el HMAC no coincide y el callback fue rechazado con 401 —
   revisar logs de `AeatCallbackController`).
2. El vendor perdió o descartó la solicitud original.
3. `external_ref` nunca se pobló para esa fila (ver el punto abierto en el
   plan: hoy `submitGroup()` no extrae ningún `external_ref` del ACK de lote —
   si es ese el caso, el callback **nunca podrá** correlacionar la fila,
   sin importar cuánto se espere).

## Detección

```sql
-- Filas en pending hace más de N minutos (ajustar el intervalo según el SLA
-- real del sistema externo; empezar por 30 min si no hay dato mejor).
SELECT id, table_key, external_ref, batch_id, last_written_at, data
FROM table_rows
WHERE submission_status = 'pending'
  AND last_written_at < now() - interval '30 minutes'
ORDER BY last_written_at ASC;
```

Para ver todas las filas de un mismo lote sospechoso de haberse perdido
entero (útil para decidir si el problema es puntual o del lote completo):

```sql
SELECT id, external_ref, submission_status, last_written_at
FROM table_rows
WHERE batch_id = '<batch_id de la fila atascada>'
ORDER BY last_written_at;
```

Filas con `external_ref IS NULL` en este listado son sospechosas de estar en
la situación del punto 3 anterior — no tiene sentido esperar un callback para
ellas hasta que se resuelva ese punto abierto del diseño.

## Remediación

- **Reintentar sin perder el intento anterior**: volver a `queued` fuerza que
  el próximo barrido (evento o programado) la reenvíe:

  ```sql
  UPDATE table_rows SET submission_status = 'queued' WHERE id = '<id>';
  ```

  Luego disparar manualmente `POST /v1/operations/table.write.batchSubmit/trigger`
  (con `MgmtTokenGuard`) para no esperar al próximo evento/cron.

- **Confirmar que el callback llegó pero no correlacionó**: buscar en los
  logs `AeatResultProcessor` la línea `no row found for external_ref="..."`
  — si aparece con el `external_ref` esperado, el problema es que la fila
  nunca tuvo ese `external_ref` asignado (punto 3), no que el callback fallara.

- **Confirmar que el HMAC no está bloqueando todo**: un `AEAT_CALLBACK_HMAC_SECRET`
  mal configurado (o ausente) rechaza el 100% de los callbacks con 401 antes
  de que lleguen a encolarse — revisar `AeatCallbackController` logs
  (`AEAT callback rejected: ...`) primero, antes de sospechar de filas
  individuales.

## Red de seguridad (mitiga, no sustituye, la investigación de arriba)

BullMQ puede no encolar un nuevo job de debounce si el anterior para el mismo
`(tableKey, groupValues)` ya está `active` (en plena llamada HTTP) cuando
llega una edición — esa edición se queda `queued` sin ningún sweep futuro que
la recoja, salvo que algo más la barra. Por eso el hub debe programar
`table.write.batchSubmit` con una cadencia baja (p. ej. cada 5-10 min) **para
todas las templates con `write` configurado, incluidas las de modo
`event`** — `TableWriteBatchService.submitAllQueued()` no distingue por
`write.trigger`, solo mira qué hay `queued` en la base de datos ahora mismo.
Esto no sustituye la investigación de una fila atascada en `pending` (ese
barrido solo recoge lo que sigue en `queued`, nunca reintenta algo que ya
recibió un ACK), pero evita que una fila se quede en `queued` para siempre.
