import { Column, Entity, PrimaryColumn } from 'typeorm';

/**
 * The satellite's hub-provisioned management token, persisted so it survives
 * restarts. Single row keyed by `key` ('mgmt-token'). `previousToken` is kept
 * only during the rotation grace window so in-flight hub calls using the
 * pre-rotation token still validate. Postgres counterpart of the Mongo
 * `provisioned_secrets` collection used by the other satellites.
 */
@Entity('provisioned_secrets')
export class ProvisionedSecret {
  @PrimaryColumn({ type: 'varchar', length: 64 })
  key!: string;

  @Column({ type: 'text' })
  token!: string;

  @Column({ type: 'bigint', name: 'issued_at' })
  issuedAt!: string;

  @Column({ type: 'text', name: 'previous_token', nullable: true })
  previousToken!: string | null;

  @Column({ type: 'bigint', name: 'previous_until', nullable: true })
  previousUntil!: string | null;
}
