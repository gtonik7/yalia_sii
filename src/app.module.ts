import { join } from 'path';
import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { TypeOrmModule, TypeOrmModuleOptions } from '@nestjs/typeorm';
import { BullModule } from '@nestjs/bullmq';
import { AppConfigModule } from './config/config.module';
import { RedisModule } from './core/redis/redis.module';
import { ProvisionedTokenModule } from './core/auth/provisioned-token.module';
import { OperationRegistryModule } from './operations/operation-registry.module';
import { JobsModule } from './jobs/jobs.module';
import { HealthModule } from './health/health.module';
import { AnnounceModule } from './announce/announce.module';
import { CapabilitiesModule } from './capabilities/capabilities.module';
import { DatasetsModule } from './datasets/datasets.module';
import { TablesModule } from './tables/tables.module';
import { ConnectionsModule } from './connections/connections.module';
import { CallbacksModule } from './callbacks/callbacks.module';
import type { Env } from './config/env';

@Module({
  imports: [
    AppConfigModule,

    TypeOrmModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (cfg: ConfigService<Env>): TypeOrmModuleOptions => {
        return {
          type: 'postgres',
          host: cfg.get('DB_HOST', { infer: true }),
          port: cfg.get('DB_PORT', { infer: true }),
          username: cfg.get('DB_USER', { infer: true }),
          password: cfg.get('DB_PASSWORD', { infer: true }),
          database: cfg.get('DB_NAME', { infer: true }),
          autoLoadEntities: true,
          // Siempre por migraciones, también en dev. El esquema tiene DDL que
          // `synchronize` no sabe reconciliar y que ningún entity declara: el
          // trigger `updated_at` (BEFORE UPDATE), la hypertable de Timescale de
          // `source_poll_runs`, y los índices únicos parciales dinámicos por
          // template (`TableIndexManagerService`). (La columna generada
          // `search_vector` existió y forzaba esto vía `typeorm_metadata`; se
          // retiró por compresión —ver DropTableRowsSearchVector— pero la
          // política migrations-only se mantiene por el resto de DDL.) Correr
          // `npm run migration:run` es, por tanto, un prerrequisito real antes
          // de `start:dev`, no solo un paso de despliegue.
          synchronize: false,
          migrations: [join(__dirname, 'migrations', '*{.js,.ts}')],
          // Las migraciones se ejecutan siempre al arrancar, para que el
          // esquema quede al día sin depender de un paso manual previo.
          migrationsRun: true,
        };
      },
    }),

    BullModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (cfg: ConfigService<Env>) => ({
        connection: {
          host: cfg.get('REDIS_HOST', { infer: true }),
          port: cfg.get('REDIS_PORT', { infer: true }),
          password: cfg.get('REDIS_PASSWORD', { infer: true }) || undefined,
        },
      }),
    }),

    RedisModule,
    ProvisionedTokenModule,
    OperationRegistryModule,
    JobsModule,
    HealthModule,
    CapabilitiesModule,
    DatasetsModule,
    TablesModule,
    ConnectionsModule,
    AnnounceModule,
    CallbacksModule,
  ],
})
export class AppModule {}
