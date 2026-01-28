import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Product } from './entities/product.entity.js';
import { GetProductsDto } from './dto/get-products.dto.js';
import { ExchangeRatesService } from '../exchange-rates/exchange-rates.service.js';

export interface ProductWithRates {
  id: string;
  name: string;
  price: number;
  expirationDate: Date;
  uploadDate: Date;
  exchangeRates: { [currency: string]: number };
}

export interface PaginatedResponse {
  success: boolean;
  data: ProductWithRates[];
  pagination: {
    page: number;
    limit: number;
    total: number;
    totalPages: number;
  };
}

@Injectable()
export class ProductsService {
  constructor(
    @InjectRepository(Product)
    private readonly productRepository: Repository<Product>,
    private readonly exchangeRatesService: ExchangeRatesService,
  ) { }

  /**
   * Retrieves products with filters, sorting, and pagination.
   * Also attaches real-time exchange rate conversions.
   */
  async findAll(query: GetProductsDto): Promise<PaginatedResponse> {
    const {
      page = 1,
      limit = 50,
      sortBy = 'name',
      sortOrder = 'ASC',
      filterName,
      priceMin,
      priceMax,
      expirationFrom,
      expirationTo,
      batchId,
    } = query;

    const qb = this.productRepository.createQueryBuilder('product');

    if (filterName) {
      qb.andWhere('LOWER(product.name) LIKE LOWER(:name)', {
        name: `%${filterName}%`,
      });
    }

    if (priceMin !== undefined) {
      qb.andWhere('product.price >= :priceMin', { priceMin });
    }

    if (priceMax !== undefined) {
      qb.andWhere('product.price <= :priceMax', { priceMax });
    }

    if (expirationFrom) {
      qb.andWhere('product.expirationDate >= :expirationFrom', {
        expirationFrom,
      });
    }

    if (expirationTo) {
      qb.andWhere('product.expirationDate <= :expirationTo', {
        expirationTo,
      });
    }

    if (batchId) {
      qb.andWhere('product.uploadBatchId = :batchId', { batchId });
    }

    if (query.userId) {
      qb.andWhere('product.userId = :userId', { userId: query.userId });
    }

    const sortColumn =
      sortBy === 'expirationDate'
        ? 'product.expirationDate'
        : sortBy === 'price'
          ? 'product.price'
          : 'product.name';
    qb.orderBy(sortColumn, sortOrder);

    const total = await qb.getCount();
    const skip = (page - 1) * limit;

    qb.skip(skip).take(limit);

    const products = await qb.getMany();

    const ratesMap: Map<string, { [currency: string]: number }> = new Map();

    if (products.length > 0) {
      const batchIds = [...new Set(products.map((p) => p.uploadBatchId))];

      for (const bid of batchIds) {
        const rates = await this.exchangeRatesService.getRatesForBatch(bid);
        const rateObj: { [currency: string]: number } = {};
        rates.forEach((r) => {
          rateObj[r.currencyCode] = Number(r.rate);
        });
        ratesMap.set(bid, rateObj);
      }
    }

    const data: ProductWithRates[] = products.map((product) => {
      const batchRates = ratesMap.get(product.uploadBatchId) || {};
      const exchangeRates: { [currency: string]: number } = {};

      for (const [currency, rate] of Object.entries(batchRates)) {
        exchangeRates[currency] =
          Math.round(Number(product.price) * rate * 100) / 100;
      }

      return {
        id: product.id,
        name: product.name,
        price: Number(product.price),
        expirationDate: product.expirationDate,
        uploadDate: product.createdAt,
        exchangeRates,
      };
    });

    return {
      success: true,
      data,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
      },
    };
  }

  /**
   * Bulk creates products efficiently.
   */
  async createMany(products: Partial<Product>[]): Promise<Product[]> {
    const entities = this.productRepository.create(products);
    return this.productRepository.save(entities);
  }
}
