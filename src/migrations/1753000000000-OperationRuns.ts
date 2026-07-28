import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * `operation_runs`: estado + resultado de las operaciones pesadas de tabla ejecutadas
 * en background (borrado masivo, count/stats/report exactos). El trigger crea la fila
 * `queued` y responde `{ runId }`; el `MaintenanceProcessor` la completa. Sustituye la
 * ejecución síncrona que podía superar el techo de timeout del proxy del hub (30 s).
 */
export class OperationRuns1753000000000 implements MigrationInterface {
    name = 'OperationRuns1753000000000';

    public async up(q: QueryRunner): Promise<void> {
        await q.query(`
            CREATE TABLE IF NOT EXISTS "operation_runs" (
                "id" uuid NOT NULL DEFAULT gen_random_uuid(),
                "operation" varchar(64) NOT NULL,
                "table_key" varchar(128),
                "status" varchar(16) NOT NULL,
                "params" jsonb NOT NULL DEFAULT '{}',
                "result" jsonb,
                "error" text,
                "created_at" timestamptz NOT NULL DEFAULT now(),
                "started_at" timestamptz,
                "finished_at" timestamptz,
                CONSTRAINT "PK_operation_runs" PRIMARY KEY ("id")
            );
        `);
        await q.query(`CREATE INDEX IF NOT EXISTS "IDX_operation_runs_operation" ON "operation_runs" ("operation");`);
        await q.query(`CREATE INDEX IF NOT EXISTS "IDX_operation_runs_table_key" ON "operation_runs" ("table_key");`);
        await q.query(`CREATE INDEX IF NOT EXISTS "IDX_operation_runs_status" ON "operation_runs" ("status");`);
        await q.query(`CREATE INDEX IF NOT EXISTS "IDX_operation_runs_created_at" ON "operation_runs" ("created_at");`);
    }

    public async down(q: QueryRunner): Promise<void> {
        await q.query(`DROP TABLE IF EXISTS "operation_runs";`);
    }
}
