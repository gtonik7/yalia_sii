declare module 'bullmq' {
  import type { Redis } from 'ioredis';

  interface JobsOptions {
    attempts?: number;
    delay?: number;
    jobId?: string;
    repeat?: {
      pattern?: string;
      every?: number;
      limit?: number;
      offset?: number;
      tz?: string;
      startDate?: Date | number;
      endDate?: Date | number;
    };
    removeOnComplete?: boolean | { age?: number };
    removeOnFail?: boolean | { age?: number };
    [key: string]: any;
  }

  class Queue<T = any> {
    constructor(name: string, opts?: any);
    add(name: string, data: T, opts?: JobsOptions): Promise<Job<T>>;
    process(handler: (job: Job<T>) => Promise<any>): void;
    on(event: string, handler: (...args: any[]) => void): void;
    close(): Promise<void>;
    getJobs(types?: string[], start?: number, end?: number, asc?: boolean): Promise<Job<T>[]>;
    getJob(jobId: string): Promise<Job<T> | undefined>;
    getJobCounts(...types: string[]): Promise<Record<string, number>>;
    obliterate(opts?: { force?: boolean }): Promise<void>;
    client?: Redis;
  }

  interface Job<T = any> {
    data: T;
    id?: string | number;
    name: string;
    progress(value: number): Promise<void>;
    updateProgress(value: number): Promise<void>;
    log(message: string): Promise<void>;
    getState(): Promise<string>;
    remove(): Promise<void>;
    retry(): Promise<void>;
    isFailed(): Promise<boolean>;
    isCompleted(): Promise<boolean>;
  }

  class Worker<T = any> {
    constructor(queueName: string, processor?: (job: Job<T>) => Promise<any>, opts?: any);
    on(event: string, handler: (...args: any[]) => void): void;
    close(): Promise<void>;
  }

  class UnrecoverableError extends Error {
    constructor(message: string);
  }

  interface WorkerOptions {
    concurrency?: number;
    lockDuration?: number;
    lockRenewTime?: number;
    maxStalledCount?: number;
    settings?: any;
  }

  interface ProcessorOptions {
    name?: string;
  }

  interface NestWorkerOptions extends WorkerOptions {}

  class WorkerHost {
    process(job: Job): Promise<any>;
  }

  export { Queue, Worker, Job, WorkerHost, WorkerOptions, ProcessorOptions, NestWorkerOptions, UnrecoverableError };
}
