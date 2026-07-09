declare module '@nestjs/bullmq' {
  import type { Job, Queue as BullQueue } from 'bullmq';
  import type { DynamicModule, ModuleMetadata } from '@nestjs/common';

  interface WorkerOptions {
    concurrency?: number;
    lockDuration?: number;
    lockRenewTime?: number;
    maxStalledCount?: number;
    settings?: any;
  }

  interface ProcessorOptions {
    concurrency?: number;
    lockDuration?: number;
    lockRenewTime?: number;
  }

  type NestWorkerOptions = WorkerOptions;

  function Processor(
    queueName: string,
    options?: WorkerOptions & ProcessorOptions,
  ): ClassDecorator;
  function Processor(
    options: (WorkerOptions & ProcessorOptions) & { name?: string },
  ): ClassDecorator;

  function InjectQueue(name: string): (...args: any[]) => any;
  function InjectCursor(name: string, cursor?: string): (...args: any[]) => any;

  class WorkerHost {
    process(job: Job): Promise<any>;
  }

  interface BullModuleOptions {
    connection?: {
      host?: string;
      port?: number;
      password?: string;
      [key: string]: any;
    };
    [key: string]: any;
  }

  interface BullModuleAsyncOptions extends Pick<ModuleMetadata, 'imports'> {
    name?: string;
    useFactory?: (...args: any[]) => BullModuleOptions | Promise<BullModuleOptions>;
    inject?: any[];
    [key: string]: any;
  }

  class BullModule {
    static forRoot(options?: BullModuleOptions): DynamicModule;
    static forRootAsync(options?: BullModuleAsyncOptions): DynamicModule;
    static registerQueue(...queues: any[]): DynamicModule;
  }

  export {
    Processor,
    WorkerHost,
    InjectQueue,
    InjectCursor,
    BullModule,
    WorkerOptions,
    ProcessorOptions,
    NestWorkerOptions,
    BullModuleOptions,
    BullModuleAsyncOptions,
  };
}
