import { Global, Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { TypeOrmModule } from '@nestjs/typeorm';
import { QUEUES } from '../queues/queues.constants';
import { ProvisionedSecret } from './provisioned-secret.entity';
import { ProvisionedTokenService } from './provisioned-token.service';
import { SatelliteControlProcessor } from './satellite-control.processor';

/**
 * Global module holding the hub-provisioned management token: a consumer of the
 * `sat-<key>-control` broker queue (persists/rotates the token) and the in-memory
 * ProvisionedTokenService the MgmtTokenGuard validates against. Global so every
 * module that registers MgmtTokenGuard can inject the service. Postgres-backed
 * counterpart of the Mongo variant in the other satellites.
 */
@Global()
@Module({
  imports: [
    BullModule.registerQueue({ name: QUEUES.CONTROL }),
    TypeOrmModule.forFeature([ProvisionedSecret]),
  ],
  providers: [ProvisionedTokenService, SatelliteControlProcessor],
  exports: [ProvisionedTokenService],
})
export class ProvisionedTokenModule {}
