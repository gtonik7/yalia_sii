import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { OperationHandler } from './operation.interface';

@Injectable()
export class OperationRegistryService {
  private readonly logger = new Logger(OperationRegistryService.name);
  private readonly registry = new Map<string, OperationHandler>();

  register(handler: OperationHandler): void {
    if (this.registry.has(handler.operationKey)) {
      throw new Error(`OperationHandler for key "${handler.operationKey}" already registered`);
    }
    this.registry.set(handler.operationKey, handler);
    this.logger.log(`Registered operation: ${handler.operationKey}`);
  }

  get(operationKey: string): OperationHandler {
    const handler = this.registry.get(operationKey);
    if (!handler) throw new NotFoundException(`No handler registered for operation "${operationKey}"`);
    return handler;
  }

  has(operationKey: string): boolean {
    return this.registry.has(operationKey);
  }

  list(): string[] {
    return Array.from(this.registry.keys());
  }
}
