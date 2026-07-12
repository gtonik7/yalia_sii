import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { QUEUES } from '../queues/queues.constants';
import { ProvisionedTokenService } from './provisioned-token.service';

/** Contract shared with the hub's SatelliteControlService. */
interface SatelliteControlMessage {
  type: 'set-mgmt-token';
  token: string;
  issuedAt: number;
}

/**
 * Consumes hub→satellite control messages from `sat-<key>-control`. Today the only
 * message is `set-mgmt-token` (provision/rotation): it persists + caches the token
 * so the MgmtTokenGuard starts accepting the hub's calls.
 */
@Processor(QUEUES.CONTROL)
export class SatelliteControlProcessor extends WorkerHost {
  private readonly logger = new Logger(SatelliteControlProcessor.name);

  constructor(private readonly tokens: ProvisionedTokenService) {
    super();
  }

  async process(job: Job<SatelliteControlMessage>): Promise<void> {
    const msg = job.data;
    if (msg?.type === 'set-mgmt-token' && typeof msg.token === 'string') {
      await this.tokens.set(msg.token, msg.issuedAt ?? Date.now());
      return;
    }
    this.logger.warn(`Ignoring unknown control message: ${JSON.stringify(msg?.type)}`);
  }
}
