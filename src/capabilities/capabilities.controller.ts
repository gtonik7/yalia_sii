import { Controller, Get, UseGuards } from '@nestjs/common';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';

export interface SatelliteUiSurface {
    key: string;
    label: string;
    kind: string;
    icon?: string;
    requiresConnection?: boolean;
    placement?: 'tab' | 'connection-action';
    config?: Record<string, unknown>;
}

export interface TriggerableOperation {
    key: string;
    label: string;
    scheduleManaged: 'hub' | 'code';
    requiresConnection: boolean;
    paramsSchema?: Record<string, unknown>;
}

/** Evento de dominio que este satélite puede emitir como origin de un Flow. */
export interface OriginOperation {
    key: string;
    label: string;
}

@Controller('v1/satellite')
@UseGuards(MgmtTokenGuard)
export class CapabilitiesController {
    @Get('capabilities')
    list(): { tabs: SatelliteUiSurface[]; triggerableOperations: TriggerableOperation[]; originOperations: OriginOperation[] } {
        return {
            // Orden de pestañas: Configuración (genérica del hub) · Conexiones · Tablas.
            tabs: [
                {
                    // Gestión de conexiones a sistemas externos (auth) que alimentan el
                    // envío saliente (`write`, cron interno por conexión). Renderer
                    // dedicado: el form genérico no representa la config anidada.
                    // yalia_sii solo admite bearer token: se restringe el selector de
                    // auth de la pestaña genérica vía `config.allowedAuthTypes`. Las
                    // tablas que consulta el envío son propias del Postgres de
                    // yalia_sii (no un sistema externo paginado) — no hay modo
                    // pull/auditoría aquí, así que se desactiva la sección de
                    // paginación del form genérico vía `config.hasPagination: false`.
                    key: 'source-connections',
                    label: 'Conexiones',
                    kind: 'source-connections',
                    requiresConnection: false,
                    config: { allowedAuthTypes: ['bearer'], siiCallback: true, hasPagination: false },
                },
                {
                    key: 'tables',
                    label: 'Tablas',
                    kind: 'table-templates',
                    requiresConnection: false,
                    // Opts into the hub FE's composite-idField picker (Campo ID as a
                    // MultiSelect instead of a single Select) — yalia_sii tables support
                    // `idFields` (composite upsert key) alongside plain `idField`.
                    config: { compositeIdField: true },
                },
                {
                    // Observabilidad de solo lectura del outbox de eventos de dominio
                    // (src/outbox/): pendientes, dead-letter y último error.
                    key: 'domain-events',
                    label: 'Eventos de dominio',
                    kind: 'domain-events',
                    requiresConnection: false,
                },
                {
                    // Backup/restore programado de la BD (src/backup/): elegir tablas,
                    // frecuencia (cron) y destinos (local/descarga/email), más restore.
                    key: 'backups',
                    label: 'Backups',
                    kind: 'backups',
                    requiresConnection: false,
                },
                {
                    // Retención de datos (src/retention/): configura ventana + cadencia de
                    // purga de las tablas de trazas/ledger (delete-events, outbox, write-runs)
                    // y permite forzar una purga en background con barra de progreso.
                    key: 'retention',
                    label: 'Retención',
                    kind: 'retention',
                    requiresConnection: false,
                },
            ],
            // Eventos de dominio que este satélite emite hacia `hub-events` (ver
            // `TableRowsService.buildEmittedEvent`). Deben quedar byte-idénticos a
            // los strings que emite ese método — no hay enum compartido, es a mano.
            originOperations: [
                { key: 'emitida.sent', label: 'Emitida enviada a SII (2xx)' },
                { key: 'emitida.error', label: 'Emitida con error de envío a SII' },
            ],
            // `table.ingest` (push) needs no trigger.
            triggerableOperations: [
                {
                    // Hub-driven sweep of a table's queued rows. Together with the
                    // per-connection internal cron (scheduled tables) and manual
                    // "Forzar envío", this is how queued rows ever get submitted.
                    key: 'table.write.batchSubmit',
                    label: 'Presentar registros pendientes (batch)',
                    scheduleManaged: 'hub',
                    requiresConnection: false,
                    paramsSchema: {
                        type: 'object',
                        required: ['tableKey'],
                        properties: {
                            tableKey: {
                                type: 'string',
                                title: 'Tabla a presentar',
                                description: 'key de la plantilla con write configurado',
                            },
                        },
                    },
                },
                {
                    // Purga de retención en background (por lotes, con progreso). El target
                    // (tabla de trazas/ledger) va en params.targetKey; ver RetentionService.
                    key: 'retention.purge',
                    label: 'Purgar retención (background)',
                    scheduleManaged: 'code',
                    requiresConnection: false,
                    paramsSchema: {
                        type: 'object',
                        required: ['targetKey'],
                        properties: {
                            targetKey: { type: 'string', title: 'Target de retención' },
                        },
                    },
                },
                {
                    // Solo disponible por tabla si la plantilla tiene allowBulkDelete —
                    // el propio controller lo re-valida server-side.
                    key: 'table.bulkDelete',
                    label: 'Borrado masivo por filtros',
                    scheduleManaged: 'code',
                    requiresConnection: false,
                    paramsSchema: {
                        type: 'object',
                        required: ['tableKey', 'filters', 'confirm'],
                        properties: {
                            tableKey: { type: 'string', title: 'Tabla' },
                            filters: { type: 'object', title: 'Filtros' },
                            connectionId: { type: 'string', title: 'Conexión' },
                            confirm: { type: 'boolean', title: 'Confirmación explícita' },
                        },
                    },
                },
                {
                    // KPI agregado de envío/estado SII de todas las tablas con write
                    // configurado — usado por el dashboard "Resumen SII" de la pestaña Tablas.
                    key: 'table.writeSummary',
                    label: 'Resumen SII (KPIs por estado)',
                    scheduleManaged: 'code',
                    requiresConnection: false,
                    paramsSchema: {
                        type: 'object',
                        properties: {
                            connectionId: { type: 'string', title: 'Conexión' },
                        },
                    },
                },
                {
                    // Conteo exacto (sin tope) bajo unos filtros — usado antes de un
                    // borrado masivo y como "contar exacto" en Registros.
                    key: 'table.count',
                    label: 'Conteo exacto por filtros',
                    scheduleManaged: 'code',
                    requiresConnection: false,
                    paramsSchema: {
                        type: 'object',
                        required: ['tableKey'],
                        properties: {
                            tableKey: { type: 'string', title: 'Tabla' },
                            filters: { type: 'object', title: 'Filtros' },
                            connectionId: { type: 'string', title: 'Conexión' },
                        },
                    },
                },
                {
                    // Informe ad-hoc: agrupa las filas de una tabla por 1..N columnas
                    // (con granularidad de fecha) y devuelve conteos + métricas numéricas.
                    // Usado por el modal "Informe" por tabla en la pestaña Tablas.
                    key: 'table.report',
                    label: 'Informe ad-hoc (group-by + métricas)',
                    scheduleManaged: 'code',
                    requiresConnection: false,
                    paramsSchema: {
                        type: 'object',
                        required: ['tableKey', 'groupBy'],
                        properties: {
                            tableKey: { type: 'string', title: 'Tabla' },
                            filters: { type: 'object', title: 'Filtros' },
                            connectionId: { type: 'string', title: 'Conexión' },
                            groupBy: { type: 'array', title: 'Dimensiones de agrupación' },
                            metrics: { type: 'array', title: 'Métricas numéricas' },
                            having: { type: 'array', title: 'Filtros sobre los grupos (HAVING)' },
                        },
                    },
                },
            ],
        };
    }
}
