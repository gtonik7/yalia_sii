import 'reflect-metadata';
import { join } from 'path';
import { DataSource } from 'typeorm';
import { loadEnv } from './env';

const env = loadEnv();

// __filename es .ts bajo el CLI de TypeORM (ts-node) y .js en la app compilada
// (dist/main.js). Node no sabe parsear .ts, así que el glob debe apuntar a
// dist/**/*.entity.js cuando corre compilado o el bootstrap (run-migrations.ts,
// invocado desde dist/main.js) revienta con un SyntaxError al hacer require().
const ext = __filename.endsWith('.ts') ? 'ts' : 'js';

export const AppDataSource = new DataSource({
  type: 'postgres',
  host: env.DB_HOST,
  port: env.DB_PORT,
  username: env.DB_USER,
  password: env.DB_PASSWORD,
  database: env.DB_NAME,
  entities: [join(__dirname, `../**/*.entity.${ext}`)],
  migrations: [join(__dirname, `../migrations/*.${ext}`)],
  synchronize: false,
});
