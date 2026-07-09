import { Controller, Headers, HttpCode, Logger, Post, Req, UnauthorizedException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectQueue } from '@nestjs/bullmq';
import type { Queue } from 'bullmq';
import { createHash, createHmac, timingSafeEqual } from 'node:crypto';
import type { FastifyRequest } from 'fastify';
import { QUEUES, DEFAULT_JOB_OPTS } from '../core/queues/queues.constants';
import type { AeatCallbackJobData } from './aeat-callback.types';
import type { Env } from '../config/env';

/**
 * Public receiver for the external vendor's AEAT-result callback.
 * NOT guarded by MgmtTokenGuard — the caller is the vendor, not the hub.
 *
 * Contract (see the plan's open risks — header name/encoding assumed, not
 * yet confirmed with the vendor):
 * 1. Verify HMAC-SHA256 (hex) over the raw body using AEAT_CALLBACK_HMAC_SECRET,
 *    carried in the `x-aeat-signature` header. `rawBody:true` in main.ts is required.
 * 2. Enqueue for async processing and respond 202 immediately.
 * 3. AeatResultProcessor does the actual correlation/update.
 */
@Controller('v1/callbacks/aeat')
export class AeatCallbackController {
  private readonly logger = new Logger(AeatCallbackController.name);

  constructor(
    private readonly config: ConfigService<Env, true>,
    @InjectQueue(QUEUES.AEAT_INBOUND) private readonly queue: Queue<AeatCallbackJobData>,
  ) {}

  @Post()
  @HttpCode(202)
  async receive(
    @Req() req: FastifyRequest & { rawBody?: Buffer },
    @Headers('x-aeat-signature') signature: string | undefined,
  ): Promise<{ ok: true }> {
    const secret = this.config.get('AEAT_CALLBACK_HMAC_SECRET', { infer: true });
    if (!secret) {
      // Fail closed: an unguarded endpoint with signing "off" would accept
      // unauthenticated writes to submission_status/aeat_response.
      this.logger.error('AEAT callback rejected: AEAT_CALLBACK_HMAC_SECRET is not configured');
      throw new UnauthorizedException('Callback signing is not configured');
    }

    const rawBody = req.rawBody;
    if (!rawBody) {
      this.logger.error('rawBody not available — ensure rawBody:true in NestFactory.create');
      throw new UnauthorizedException('Signature verification failed: no raw body');
    }
    if (!signature) {
      throw new UnauthorizedException('Missing x-aeat-signature header');
    }

    const expected = createHmac('sha256', secret).update(rawBody).digest('hex');
    const a = Buffer.from(signature);
    const b = Buffer.from(expected);
    if (a.length !== b.length || !timingSafeEqual(a, b)) {
      this.logger.warn('AEAT callback rejected: signature mismatch');
      throw new UnauthorizedException('Invalid signature');
    }

    const payload = req.body ?? {};
    // Dedupe on body content (no vendor-issued delivery id is known/confirmed
    // yet) so an identical retry doesn't reprocess the same result twice.
    const dedupeId = `aeat_cb_${createHash('sha256').update(rawBody).digest('hex').slice(0, 24)}`;

    await this.queue.add(
      'aeat.callback',
      { payload } satisfies AeatCallbackJobData,
      { ...DEFAULT_JOB_OPTS, jobId: dedupeId, removeOnComplete: true },
    );

    return { ok: true };
  }
}
