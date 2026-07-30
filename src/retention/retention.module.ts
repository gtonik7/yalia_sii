import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { RetentionSetting } from './entities/retention-setting.entity';
import { RetentionService } from './retention.service';
import { RetentionController } from './retention.controller';

/**
 * Config + mecánica de purga de los targets de retención del satélite. RetentionService
 * se exporta para que el MaintenanceProcessor (operación `retention.purge`) y el
 * RetentionScheduler (ambos en TablesModule, donde vive OperationRunService) lo usen —
 * así se evita un ciclo de módulos (RetentionModule no depende de TablesModule).
 */
@Module({
  imports: [TypeOrmModule.forFeature([RetentionSetting])],
  providers: [RetentionService],
  controllers: [RetentionController],
  exports: [RetentionService],
})
export class RetentionModule {}
