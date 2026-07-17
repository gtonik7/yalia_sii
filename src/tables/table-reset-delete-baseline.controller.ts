import { Controller, HttpCode, Post, UseGuards } from '@nestjs/common';
import { MgmtTokenGuard } from '../core/auth/mgmt-token.guard';
import { TableRowsService } from './table-rows.service';

/**
 * Re-baseline del contador global de "borrados no controlados", usado por el botón
 * "Reiniciar contador" de la página "Conciliación" del hub-fe:
 *   POST /v1/operations/table.resetDeleteBaseline/trigger
 * → { deletedSinceLoad, voluntaryDeletes, uncontrolledDeletes, rebaselinedBy }.
 * Reutiliza el mismo proxy genérico `/satellites/:key/operations/:operationKey/trigger`
 * del hub que table.stats (sin ruta nueva en el hub). Es una mutación (inserta una
 * fila `baseline`), pero no recibe parámetros: el contador es global a table_rows.
 */
@UseGuards(MgmtTokenGuard)
@Controller('v1/operations/table.resetDeleteBaseline')
export class TableResetDeleteBaselineController {
  constructor(private readonly rows: TableRowsService) {}

  @Post('trigger')
  @HttpCode(200)
  async trigger(): Promise<{ deletedSinceLoad: number; voluntaryDeletes: number; uncontrolledDeletes: number; rebaselinedBy: number }> {
    return this.rows.resetDeleteBaseline();
  }
}
