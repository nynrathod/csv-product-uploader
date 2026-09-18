import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bull';
import { TypeOrmModule } from '@nestjs/typeorm';
import { CsvProcessor } from './csv.processor.js';
import { UploadBatch } from '../upload/entities/upload-batch.entity.js';
import { Product } from '../products/entities/product.entity.js';
import { UploadModule } from '../upload/upload.module.js';
import { ExchangeRatesModule } from '../exchange-rates/exchange-rates.module.js';

@Module({
  imports: [
    BullModule.registerQueue({
      name: 'csv-processing',
    }),
    TypeOrmModule.forFeature([UploadBatch, Product]),
    UploadModule,
    ExchangeRatesModule,
  ],
  providers: [CsvProcessor],
})
export class JobsModule {}
