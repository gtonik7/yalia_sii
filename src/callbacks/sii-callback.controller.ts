import { Controller, Headers, HttpCode, Logger, Param, Post, Req, UnauthorizedException } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import type { Queue } from 'bullmq';
import { createHash, createHmac, timingSafeEqual } from 'node:crypto';
import type { FastifyRequest } from 'fastify';
import { QUEUES, DEFAULT_JOB_OPTS } from '../core/queues/queues.constants';
import type { SiiCallbackJobData } from './sii-callback.types';
import { SourceConnectionsService } from '../connections/source-connections.service';

/**
 * Public receiver for the external vendor's SII-result callback, one per
 * connection. NOT guarded by MgmtTokenGuard — the caller is the vendor, not the hub.
 *
 * Contract (see the plan's open risks — header name/encoding assumed, not
 * yet confirmed with the vendor):
 * 1. Look up the connection's own callback secret (auto-generated, rotatable via
 *    the connections UI) and verify HMAC-SHA256 (hex) over the raw body, carried
 *    in the `x-sii-signature` header. `rawBody:true` in main.ts is required.
 * 2. Enqueue for async processing and respond 202 immediately.
 * 3. SiiResultProcessor does the actual correlation/update.
 */
@Controller('v1/callbacks/sii')
export class SiiCallbackController {
  private readonly logger = new Logger(SiiCallbackController.name);

  constructor(
    private readonly connections: SourceConnectionsService,
    @InjectQueue(QUEUES.SII_INBOUND) private readonly queue: Queue<SiiCallbackJobData>,
  ) {}

  @Post(':connectionId')
  @HttpCode(202)
  async receive(
    @Param('connectionId') connectionId: string,
    @Req() req: FastifyRequest & { rawBody?: Buffer },
    @Headers('x-sii-signature') signature: string | undefined,
  ): Promise<{ ok: true }> {
    const secret = await this.connections.findCallbackSecret(connectionId);
    if (!secret) {
      // Fail closed: an unguarded endpoint with signing "off" would accept
      // unauthenticated writes to submission_status/sii_response.
      this.logger.error(`SII callback rejected: no callback secret for connection ${connectionId}`);
      throw new UnauthorizedException('Callback signing is not configured for this connection');
    }

    const rawBody = req.rawBody;
    if (!rawBody) {
      this.logger.error('rawBody not available — ensure rawBody:true in NestFactory.create');
      throw new UnauthorizedException('Signature verification failed: no raw body');
    }
    if (!signature) {
      throw new UnauthorizedException('Missing x-sii-signature header');
    }

    const expected = createHmac('sha256', secret).update(rawBody).digest('hex');
    const a = Buffer.from(signature);
    const b = Buffer.from(expected);
    if (a.length !== b.length || !timingSafeEqual(a, b)) {
      this.logger.warn('SII callback rejected: signature mismatch');
      throw new UnauthorizedException('Invalid signature');
    }

    const payload = req.body ?? {};
    // Dedupe on body content (no vendor-issued delivery id is known/confirmed
    // yet) so an identical retry doesn't reprocess the same result twice.
    const dedupeId = `sii_cb_${createHash('sha256').update(rawBody).digest('hex').slice(0, 24)}`;

    await this.queue.add(
      'sii.callback',
      { payload } satisfies SiiCallbackJobData,
      { ...DEFAULT_JOB_OPTS, jobId: dedupeId, removeOnComplete: true },
    );

    return { ok: true };
  }
}
