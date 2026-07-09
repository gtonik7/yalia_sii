import { createHmac } from 'node:crypto';
import { UnauthorizedException } from '@nestjs/common';
import type { FastifyRequest } from 'fastify';
import { AeatCallbackController } from './aeat-callback.controller';

const SECRET = 'a-test-secret-at-least-16-chars';

function sign(body: Buffer): string {
  return createHmac('sha256', SECRET).update(body).digest('hex');
}

function fakeReq(body: Record<string, unknown>): FastifyRequest & { rawBody?: Buffer } {
  const rawBody = Buffer.from(JSON.stringify(body));
  return { rawBody, body } as unknown as FastifyRequest & { rawBody?: Buffer };
}

describe('AeatCallbackController', () => {
  let addMock: jest.Mock;
  let configGetMock: jest.Mock;
  let controller: AeatCallbackController;

  function build(secret: string | undefined) {
    addMock = jest.fn().mockResolvedValue({ id: 'job-1' });
    configGetMock = jest.fn().mockReturnValue(secret);
    const fakeConfig = { get: configGetMock };
    const fakeQueue = { add: addMock };
    controller = new AeatCallbackController(fakeConfig as never, fakeQueue as never);
  }

  it('rejects with 401 and enqueues nothing when AEAT_CALLBACK_HMAC_SECRET is not configured', async () => {
    build(undefined);
    const req = fakeReq({ state: 'ERROR', invoiceId: 'a1' });

    await expect(controller.receive(req, sign(req.rawBody!))).rejects.toThrow(UnauthorizedException);
    expect(addMock).not.toHaveBeenCalled();
  });

  it('rejects with 401 when the rawBody is unavailable', async () => {
    build(SECRET);
    const req = { rawBody: undefined, body: {} } as unknown as FastifyRequest & { rawBody?: Buffer };

    await expect(controller.receive(req, 'whatever')).rejects.toThrow(UnauthorizedException);
    expect(addMock).not.toHaveBeenCalled();
  });

  it('rejects with 401 when the signature header is missing', async () => {
    build(SECRET);
    const req = fakeReq({ state: 'ERROR', invoiceId: 'a1' });

    await expect(controller.receive(req, undefined)).rejects.toThrow(UnauthorizedException);
    expect(addMock).not.toHaveBeenCalled();
  });

  it('rejects with 401 on a signature mismatch', async () => {
    build(SECRET);
    const req = fakeReq({ state: 'ERROR', invoiceId: 'a1' });

    await expect(controller.receive(req, 'deadbeef'.repeat(8))).rejects.toThrow(UnauthorizedException);
    expect(addMock).not.toHaveBeenCalled();
  });

  it('accepts a validly-signed request, enqueues the payload, and returns ok', async () => {
    build(SECRET);
    const req = fakeReq({ state: 'ERROR', invoiceId: 'a1' });

    const result = await controller.receive(req, sign(req.rawBody!));

    expect(result).toEqual({ ok: true });
    expect(addMock).toHaveBeenCalledTimes(1);
    const [name, data, opts] = addMock.mock.calls[0];
    expect(name).toBe('aeat.callback');
    expect(data).toEqual({ payload: { state: 'ERROR', invoiceId: 'a1' } });
    expect(opts).toMatchObject({ jobId: expect.stringMatching(/^aeat_cb_/), removeOnComplete: true });
  });

  it('derives the same dedupe jobId for an identical retried payload', async () => {
    build(SECRET);
    const req1 = fakeReq({ state: 'ERROR', invoiceId: 'a1' });
    const req2 = fakeReq({ state: 'ERROR', invoiceId: 'a1' });

    await controller.receive(req1, sign(req1.rawBody!));
    await controller.receive(req2, sign(req2.rawBody!));

    const jobId1 = addMock.mock.calls[0][2].jobId;
    const jobId2 = addMock.mock.calls[1][2].jobId;
    expect(jobId1).toBe(jobId2);
  });
});
