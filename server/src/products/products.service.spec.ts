import { Test, TestingModule } from '@nestjs/testing';
import { ProductsService } from './products.service';
import { getRepositoryToken } from '@nestjs/typeorm';
import { Product } from './entities/product.entity';
import { ExchangeRatesService } from '../exchange-rates/exchange-rates.service';

describe('ProductsService', () => {
  let service: ProductsService;

  const mockQb = {
    andWhere: jest.fn().mockReturnThis(),
    orderBy: jest.fn().mockReturnThis(),
    skip: jest.fn().mockReturnThis(),
    take: jest.fn().mockReturnThis(),
    getCount: jest.fn().mockResolvedValue(100),
    getMany: jest.fn().mockResolvedValue([
      {
        id: 'p1',
        name: 'Test Product',
        price: 99.99,
        expirationDate: new Date('2024-12-31'),
        uploadBatchId: 'batch-1',
        createdAt: new Date(),
      },
    ]),
  };

  const mockProductRepository = {
    createQueryBuilder: jest.fn().mockReturnValue(mockQb),
    create: jest.fn(),
    save: jest.fn(),
  };

  const mockExchangeRatesService = {
    getRatesForBatch: jest.fn().mockResolvedValue([
      { currencyCode: 'USD', rate: 1 },
      { currencyCode: 'EUR', rate: 0.92 },
    ]),
  };

  beforeEach(async () => {
    jest.clearAllMocks();
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ProductsService,
        {
          provide: getRepositoryToken(Product),
          useValue: mockProductRepository,
        },
        { provide: ExchangeRatesService, useValue: mockExchangeRatesService },
      ],
    }).compile();

    service = module.get<ProductsService>(ProductsService);
  });

  describe('findAll', () => {
    it('should return paginated products with exchange rates', async () => {
      const result = await service.findAll({
        page: 1,
        limit: 50,
        sortBy: 'name',
        sortOrder: 'ASC',
      });

      expect(result.success).toBe(true);
      expect(result.data).toHaveLength(1);
      expect(result.data[0].exchangeRates).toBeDefined();
      expect(result.pagination.total).toBe(100);
    });

    it('should apply name filter', async () => {
      await service.findAll({
        page: 1,
        limit: 50,
        sortBy: 'name',
        sortOrder: 'ASC',
        filterName: 'test',
      });

      const qb = mockProductRepository.createQueryBuilder();
      expect(qb.andWhere).toHaveBeenCalled();
    });
  });
});
