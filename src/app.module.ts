import { join } from 'path';
import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { TypeOrmModule, TypeOrmModuleOptions } from '@nestjs/typeorm';
import { BullModule } from '@nestjs/bullmq';
import { AppConfigModule } from './config/config.module';
import { RedisModule } from './core/redis/redis.module';
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
          // Siempre por migraciones, también en dev — a diferencia de yalia_hub
          // (donde `synchronize` en dev nunca carga el DDL específico de
          // Timescale), aquí `search_vector` (columna generada, creada por SQL
          // crudo en la migración) es parte del esquema desde el arranque, y
          // `synchronize` no sabe reconciliar una columna generada que no
          // declara ningún entity: su introspección de esquema consulta la
          // tabla `typeorm_metadata` (que TypeORM solo autocrea cuando ES ÉL
          // quien posee metadata de columnas generadas/vistas) y revienta con
          // "relation typeorm_metadata does not exist". Correr
          // `npm run migration:run` es, por tanto, un prerrequisito real antes
          // de `start:dev`, no solo un paso de despliegue.
          synchronize: false,
          migrations: [join(__dirname, 'migrations', '*{.js,.ts}')],
          // Ejecuta migraciones al arrancar solo si se pide explícitamente
          // (p.ej. DB_MIGRATIONS_RUN=true); por defecto se corren como paso de
          // despliegue con `npm run migration:run`. No forma parte del schema
          // Zod (deploy-only flag), se lee directo de process.env.
          migrationsRun: process.env.DB_MIGRATIONS_RUN === 'true',
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
