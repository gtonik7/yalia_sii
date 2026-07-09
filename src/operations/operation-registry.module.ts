import { Global, Module } from '@nestjs/common';
import { OperationRegistryService } from './operation-registry.service';

@Global()
@Module({
  providers: [OperationRegistryService],
  exports: [OperationRegistryService],
})
export class OperationRegistryModule {}
