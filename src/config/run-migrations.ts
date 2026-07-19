import { Migration } from 'typeorm';
import { AppDataSource } from './data-source';

export async function runMigrations(): Promise<void> {
  try {
    console.log('[Migrations] Running pending migrations...');

    if (!AppDataSource.isInitialized) {
      await AppDataSource.initialize();
    }

    const migrations: Migration[] = await AppDataSource.runMigrations();

    if (migrations.length > 0) {
      console.log(`[Migrations] Executed ${migrations.length} migration(s)`);
      migrations.forEach((m) => console.log(`  ✓ ${m.name}`));
    } else {
      console.log('[Migrations] No pending migrations');
    }
  } catch (err) {
    console.error('[Migrations] Failed:', err);
    throw err;
  }
}
