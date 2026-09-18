import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { BullModule } from '@nestjs/bull';
import { UploadController } from './upload.controller.js';
import { UploadService } from './upload.service.js';
import { UploadBatch } from './entities/upload-batch.entity.js';

@Module({
  imports: [
    TypeOrmModule.forFeature([UploadBatch]),
    BullModule.registerQueue({
      name: 'csv-processing',
    }),
  ],
  controllers: [UploadController],
  providers: [UploadService],
  exports: [UploadService],
})
export class UploadModule {}
