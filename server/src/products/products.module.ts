import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ProductsController } from './products.controller.js';
import { ProductsService } from './products.service.js';
import { Product } from './entities/product.entity.js';
import { ExchangeRatesModule } from '../exchange-rates/exchange-rates.module.js';

@Module({
  imports: [TypeOrmModule.forFeature([Product]), ExchangeRatesModule],
  controllers: [ProductsController],
  providers: [ProductsService],
  exports: [ProductsService],
})
export class ProductsModule {}
