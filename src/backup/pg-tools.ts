import { createReadStream, createWriteStream } from 'fs';
import { Client } from 'pg';
import { to as copyTo, from as copyFrom } from 'pg-copy-streams';

export interface PgConn {
    host: string;
    port: number;
    user: string;
    password: string;
    database: string;
}

function connect(conn: PgConn): Client {
    return new Client({
        host: conn.host,
        port: conn.port,
        user: conn.user,
        password: conn.password,
        database: conn.database,
    });
}

function writeChunk(stream: NodeJS.WritableStream, chunk: Buffer): Promise<void> {
    return new Promise((resolve, reject) => {
        const len = Buffer.alloc(4);
        len.writeUInt32LE(chunk.length, 0);
        stream.write(len, (err) => (err ? reject(err) : undefined));
        if (chunk.length === 0) {
            resolve();
            return;
        }
        stream.write(chunk, (err) => (err ? reject(err) : resolve()));
    });
}

/**
 * Vuelca las `tables` indicadas (o toda la BD si está vacío) a `outPath` usando
 * `COPY ... TO STDOUT WITH (FORMAT binary)` sobre una conexión `pg` normal — sin
 * invocar ningún binario externo. Contenedor propio, streaming por tabla:
 * `[uint32 len][nombre utf8]` seguido de chunks `[uint32 len][bytes]` terminados
 * por un chunk de longitud 0.
 */
export async function dumpTables(conn: PgConn, tables: string[], outPath: string): Promise<void> {
    const client = connect(conn);
    await client.connect();
    const out = createWriteStream(outPath);
    try {
        for (const table of tables) {
            const nameBuf = Buffer.from(table, 'utf8');
            const nameLen = Buffer.alloc(4);
            nameLen.writeUInt32LE(nameBuf.length, 0);
            await new Promise<void>((resolve, reject) => {
                out.write(nameLen, (err) => (err ? reject(err) : undefined));
                out.write(nameBuf, (err) => (err ? reject(err) : resolve()));
            });

            const copyStream = client.query(copyTo(`COPY "${table}" TO STDOUT WITH (FORMAT binary)`));
            for await (const chunk of copyStream as AsyncIterable<Buffer>) {
                await writeChunk(out, chunk);
            }
            await writeChunk(out, Buffer.alloc(0));
        }
        await new Promise<void>((resolve, reject) => out.end((err?: Error | null) => (err ? reject(err) : resolve())));
    } finally {
        await client.end();
    }
}

async function readExact(source: AsyncIterator<Buffer>, leftover: Buffer[], leftoverLen: { n: number }, size: number): Promise<Buffer | null> {
    while (leftoverLen.n < size) {
        const { value, done } = await source.next();
        if (done) return null;
        leftover.push(value);
        leftoverLen.n += value.length;
    }
    let buf = leftover.length === 1 ? leftover[0] : Buffer.concat(leftover, leftoverLen.n);
    const result = buf.subarray(0, size);
    const rest = buf.subarray(size);
    leftover.length = 0;
    if (rest.length) leftover.push(rest);
    leftoverLen.n = rest.length;
    return Buffer.from(result);
}

/**
 * Restaura las `tables` indicadas (o todas las presentes en el artefacto si el
 * array está vacío) desde un fichero generado por `dumpTables`. Desactiva los
 * triggers de cada tabla mientras dura su `COPY FROM STDIN` — equivalente a
 * `pg_restore --disable-triggers`, pero solo requiere ser owner de la tabla.
 */
export async function restoreTables(conn: PgConn, tables: string[], filePath: string): Promise<void> {
    const client = connect(conn);
    await client.connect();
    try {
        const fileStream = createReadStream(filePath);
        const source = fileStream[Symbol.asyncIterator]() as AsyncIterator<Buffer>;
        const leftover: Buffer[] = [];
        const leftoverLen = { n: 0 };
        const wanted = tables.length ? new Set(tables) : null;

        for (;;) {
            const nameLenBuf = await readExact(source, leftover, leftoverLen, 4);
            if (!nameLenBuf) break;
            const nameLen = nameLenBuf.readUInt32LE(0);
            const nameBuf = await readExact(source, leftover, leftoverLen, nameLen);
            if (!nameBuf) throw new Error('Artefacto de backup truncado (nombre de tabla incompleto).');
            const table = nameBuf.toString('utf8');
            const restoreThis = !wanted || wanted.has(table);

            let copyStream: NodeJS.WritableStream | null = null;
            if (restoreThis) {
                await client.query(`ALTER TABLE "${table}" DISABLE TRIGGER ALL`);
                copyStream = client.query(copyFrom(`COPY "${table}" FROM STDIN WITH (FORMAT binary)`));
            }
            try {
                for (;;) {
                    const chunkLenBuf = await readExact(source, leftover, leftoverLen, 4);
                    if (!chunkLenBuf) throw new Error(`Artefacto de backup truncado (datos de '${table}' incompletos).`);
                    const chunkLen = chunkLenBuf.readUInt32LE(0);
                    if (chunkLen === 0) break;
                    const chunk = await readExact(source, leftover, leftoverLen, chunkLen);
                    if (!chunk) throw new Error(`Artefacto de backup truncado (datos de '${table}' incompletos).`);
                    if (copyStream) await new Promise<void>((resolve, reject) => copyStream!.write(chunk, (err) => (err ? reject(err) : resolve())));
                }
                if (copyStream) await new Promise<void>((resolve, reject) => copyStream!.end((err?: Error | null) => (err ? reject(err) : resolve())));
            } finally {
                if (restoreThis) await client.query(`ALTER TABLE "${table}" ENABLE TRIGGER ALL`);
            }
        }
    } finally {
        await client.end();
    }
}
