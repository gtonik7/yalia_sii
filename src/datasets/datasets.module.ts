import { Global, Module } from '@nestjs/common';
import { DatasetRegistryService } from './dataset-registry.service';
import { DatasetsController } from './datasets.controller';

/**
 * Generic dataset exposure. Providers register themselves into
 * DatasetRegistryService on bootstrap. Global so any feature module can inject
 * the registry without an explicit import edge.
 */
@Global()
@Module({
  controllers: [DatasetsController],
  providers: [DatasetRegistryService],
  exports: [DatasetRegistryService],
})
export class DatasetsModule {}
