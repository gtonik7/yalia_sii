import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Añade columnas de progreso a `operation_runs` para las operaciones largas (purgas
 * de retención): el borrado se hace por lotes y va empujando `processed`/`total`/
 * `progress`/`phase`, de modo que el FE muestra una barra de progreso real en vez de
 * un simple spinner. Las operaciones cortas ya existentes (count/stats/report) las
 * dejan en su default (0) — no las usan.
 */
export class OperationRunsProgress1753200000000 implements MigrationInterface {
    name = 'OperationRunsProgress1753200000000';

    public async up(q: QueryRunner): Promise<void> {
        await q.query(`ALTER TABLE "operation_runs" ADD COLUMN IF NOT EXISTS "total" int NOT NULL DEFAULT 0;`);
        await q.query(`ALTER TABLE "operation_runs" ADD COLUMN IF NOT EXISTS "processed" int NOT NULL DEFAULT 0;`);
        await q.query(`ALTER TABLE "operation_runs" ADD COLUMN IF NOT EXISTS "progress" int NOT NULL DEFAULT 0;`);
        await q.query(`ALTER TABLE "operation_runs" ADD COLUMN IF NOT EXISTS "phase" varchar(32);`);
    }

    public async down(q: QueryRunner): Promise<void> {
        await q.query(`ALTER TABLE "operation_runs" DROP COLUMN IF EXISTS "phase";`);
        await q.query(`ALTER TABLE "operation_runs" DROP COLUMN IF EXISTS "progress";`);
        await q.query(`ALTER TABLE "operation_runs" DROP COLUMN IF EXISTS "processed";`);
        await q.query(`ALTER TABLE "operation_runs" DROP COLUMN IF EXISTS "total";`);
    }
}
