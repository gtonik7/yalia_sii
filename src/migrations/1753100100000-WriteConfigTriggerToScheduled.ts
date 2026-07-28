import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Replaces the old `write.trigger` ('event' | 'schedule') on every table
 * template with a boolean `write.scheduled`. The per-edit "event" send mode was
 * removed: a table now only sends via the connection's write cron (when
 * `scheduled` is true) or via manual "Forzar envío".
 *
 * Conversion (per the product decision): old `'schedule'` → `scheduled = true`
 * (keeps being swept by the cron); old `'event'` → `scheduled = false` (stops
 * auto-sending, becomes manual-only). `trigger` is dropped from the jsonb.
 */
export class WriteConfigTriggerToScheduled1753100100000 implements MigrationInterface {
  name = 'WriteConfigTriggerToScheduled1753100100000';

  public async up(q: QueryRunner): Promise<void> {
    await q.query(`
      UPDATE table_templates
      SET write = jsonb_set(write - 'trigger', '{scheduled}', to_jsonb(write->>'trigger' = 'schedule'))
      WHERE write IS NOT NULL AND write ? 'trigger';
    `);
  }

  public async down(q: QueryRunner): Promise<void> {
    // Reverse mapping: scheduled=true → 'schedule', scheduled=false → 'event'.
    await q.query(`
      UPDATE table_templates
      SET write = jsonb_set(write - 'scheduled', '{trigger}',
                            to_jsonb(CASE WHEN (write->>'scheduled')::boolean THEN 'schedule' ELSE 'event' END))
      WHERE write IS NOT NULL AND write ? 'scheduled';
    `);
  }
}
