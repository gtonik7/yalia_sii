import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  PORT: z.coerce.number().int().positive().default(5600),

  DB_HOST: z.string().default('localhost'),
  DB_PORT: z.coerce.number().int().positive().default(5434),
  DB_USER: z.string().default('yalia'),
  DB_PASSWORD: z.string().default('yalia'),
  DB_NAME: z.string().default('yalia_sii'),

  REDIS_HOST: z.string().default('localhost'),
  REDIS_PORT: z.coerce.number().int().positive().default(6379),
  REDIS_PASSWORD: z.string().optional(),

  SATELLITE_KEY: z
    .string()
    .min(1)
    .regex(/^[a-z0-9-]+$/, 'SATELLITE_KEY: solo minúsculas, dígitos y guiones')
    .default('sii'),

  SATELLITE_NAME: z.string().optional(),
  SATELLITE_HOST: z.string().optional(),
  SATELLITE_MGMT_URL: z.string().url().optional(),

  WORKER_CONCURRENCY: z.coerce.number().int().positive().default(5),

  /** Debounce window for the write-sweep queue: how long a burst of inserts/edits on the same batch group waits before submitGroup() actually fires. */
  WRITE_SWEEP_DEBOUNCE_MS: z.coerce.number().int().positive().default(5000),

  SATELLITE_MGMT_TOKEN: z.string().min(32).optional(),

  HUB_URL: z.string().url().optional(),

  /**
   * 32-byte hex key used to AES-256-GCM the source-connection credentials.
   * Required only when audit/polling connections are configured; the crypto
   * helper throws at use time if a connection needs it and it is unset.
   */
  CREDENTIALS_ENC_KEY: z
    .string()
    .regex(/^[0-9a-fA-F]{64}$/, 'CREDENTIALS_ENC_KEY must be 32 bytes hex (64 chars)')
    .optional(),

  /**
   * Shared secret for HMAC-SHA256-signing the inbound AEAT-result callback
   * (unguarded — no MgmtTokenGuard, since the caller is the external vendor,
   * not the hub). Optional in the schema, but the callback controller refuses
   * every request (401) while it's unset — fails closed, not open.
   */
  AEAT_CALLBACK_HMAC_SECRET: z.string().min(16).optional(),
});

export type Env = z.infer<typeof envSchema>;

export function loadEnv(raw: NodeJS.ProcessEnv = process.env): Env {
  const parsed = envSchema.safeParse(raw);
  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((i) => `  - ${i.path.join('.')}: ${i.message}`)
      .join('\n');
    throw new Error(`Invalid environment variables:\n${issues}`);
  }
  return parsed.data;
}
