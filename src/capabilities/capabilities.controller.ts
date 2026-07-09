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

@Controller('v1/satellite')
@UseGuards(MgmtTokenGuard)
export class CapabilitiesController {
  @Get('capabilities')
  list(): { tabs: SatelliteUiSurface[]; triggerableOperations: TriggerableOperation[] } {
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
        },
        {
          // Visor de registros en contexto del satélite: ver/editar filas de
          // cada tabla, su estado de presentación SII (`submission_status`) y
          // forzar el envío de las que están en cola. Alternativa in-place al
          // Explorador global; consume el mismo contrato `/v1/datasets`.
          key: 'records',
          label: 'Registros',
          kind: 'table-records',
          requiresConnection: false,
        },
      ],
      // `table.ingest` (push) needs no trigger.
      triggerableOperations: [
        {
          // Schedule-mode counterpart to the debounced event-mode sweep: for
          // templates with write.trigger==='schedule', this is the only thing
          // that ever submits their queued rows.
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
      ],
    };
  }
}
