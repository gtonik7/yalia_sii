import { Global, Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';
import type { Env } from '../../config/env';

export const REDIS_CLIENT = 'REDIS_CLIENT';

@Global()
@Module({
  providers: [
    {
      provide: REDIS_CLIENT,
      inject: [ConfigService],
      useFactory: (cfg: ConfigService<Env, true>): Redis => {
        return new Redis({
          host: cfg.get('REDIS_HOST', { infer: true }),
          port: cfg.get('REDIS_PORT', { infer: true }),
          password: cfg.get('REDIS_PASSWORD', { infer: true }) || undefined,
          maxRetriesPerRequest: null,
          lazyConnect: false,
        });
      },
    },
  ],
  exports: [REDIS_CLIENT],
})
export class RedisModule {}
