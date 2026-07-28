import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as nodemailer from 'nodemailer';
import type { Transporter } from 'nodemailer';
import type { Env } from '../config/env';

export interface MailAttachment {
    filename: string;
    path: string;
}

/**
 * Envío de correo por SMTP (greenfield en este satélite). Sólo se usa para
 * adjuntar artefactos de backup. Si SMTP no está configurado, `send` lanza un
 * error claro en lugar de fallar en silencio — el destino email de un backup se
 * marca como fallido y el resto de destinos siguen su curso.
 */
@Injectable()
export class MailService {
    private readonly logger = new Logger(MailService.name);
    private transporter: Transporter | null = null;

    constructor(private readonly config: ConfigService<Env, true>) {}

    isConfigured(): boolean {
        return Boolean(this.config.get('SMTP_HOST', { infer: true }));
    }

    private getTransporter(): Transporter {
        if (this.transporter) return this.transporter;
        const host = this.config.get('SMTP_HOST', { infer: true });
        if (!host) throw new Error('SMTP no configurado (falta SMTP_HOST).');
        const user = this.config.get('SMTP_USER', { infer: true });
        const pass = this.config.get('SMTP_PASS', { infer: true });
        this.transporter = nodemailer.createTransport({
            host,
            port: this.config.get('SMTP_PORT', { infer: true }),
            secure: this.config.get('SMTP_SECURE', { infer: true }),
            auth: user ? { user, pass } : undefined,
        });
        return this.transporter;
    }

    async send(opts: { to: string[]; subject: string; text: string; attachments?: MailAttachment[] }): Promise<void> {
        const from = this.config.get('SMTP_FROM', { infer: true }) || this.config.get('SMTP_USER', { infer: true });
        if (!from) throw new Error('SMTP no configurado (falta SMTP_FROM o SMTP_USER).');
        if (!opts.to.length) throw new Error('No hay destinatarios de correo.');
        await this.getTransporter().sendMail({
            from,
            to: opts.to.join(', '),
            subject: opts.subject,
            text: opts.text,
            attachments: opts.attachments,
        });
        this.logger.log(`Correo enviado a ${opts.to.join(', ')} (${opts.subject})`);
    }
}
