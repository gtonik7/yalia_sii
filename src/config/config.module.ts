import { Global, Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { loadEnv } from './env';

@Global()
@Module({
  imports: [
    ConfigModule.forRoot({
      load: [() => loadEnv()],
      isGlobal: true,
    }),
  ],
})
export class AppConfigModule {}
