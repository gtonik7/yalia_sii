// DEBE ir primero: carga `.env` en process.env ANTES de importar AppModule.
// `QUEUES.JOBS` se resuelve al evaluar los decoradores durante ese import,
// antes de que @nestjs/config parsee el .env.
import 'dotenv/config';
import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, NestFastifyApplication } from '@nestjs/platform-fastify';
import { ValidationPipe } from '@nestjs/common';
import { AppModule } from './app.module';
import { loadEnv } from './config/env';
import { QUEUES, satelliteJobsQueueName } from './core/queues/queues.constants';

async function bootstrap() {
  const env = loadEnv();

  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter({
      logger: env.NODE_ENV === 'development',
      forceCloseConnections: true,
      bodyLimit: 10 * 1024 * 1024,
    }),
    // rawBody:true es obligatorio para verificar el HMAC del callback de SII
    // (src/callbacks) — sin esto el body llega ya parseado y la firma no coincide.
    { rawBody: true },
  );
  app.enableShutdownHooks();

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: false,
      transform: true,
    }),
  );

  const server = app.getHttpServer();
  server.keepAliveTimeout = 5000;

  await app.listen(env.PORT, '0.0.0.0');

  const expected = satelliteJobsQueueName(env.SATELLITE_KEY);
  if (QUEUES.JOBS !== expected) {
    console.warn(
      `[SATELLITE_KEY] desajuste: worker escucha '${QUEUES.JOBS}' pero la ` +
        `config validada esperaba '${expected}'. ¿SATELLITE_KEY definido solo ` +
        `tras la carga de módulo?`,
    );
  }

  console.log(
    `yalia_sii satellite key=${env.SATELLITE_KEY} queue=${QUEUES.JOBS} running on port ${env.PORT}`,
  );

  process.on('SIGTERM', () => {
    console.log('SIGTERM received, closing gracefully...');
    void app.close().then(() => process.exit(0));
  });

  process.on('SIGINT', () => {
    console.log('SIGINT received, closing gracefully...');
    void app.close().then(() => process.exit(0));
  });
}

void bootstrap();
