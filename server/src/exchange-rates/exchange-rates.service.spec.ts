import { Test, TestingModule } from '@nestjs/testing';
import { ExchangeRatesService } from './exchange-rates.service';
import { getRepositoryToken } from '@nestjs/typeorm';
import { ExchangeRate } from './entities/exchange-rate.entity';

describe('ExchangeRatesService', () => {
  let service: ExchangeRatesService;

  const mockRepository = {
    create: jest.fn(),
    save: jest.fn(),
    find: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ExchangeRatesService,
        { provide: getRepositoryToken(ExchangeRate), useValue: mockRepository },
      ],
    }).compile();

    service = module.get<ExchangeRatesService>(ExchangeRatesService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('fetchRates', () => {
    it('should return rates (fallback if API fails)', async () => {
      const rates = await service.fetchRates();

      expect(rates).toBeDefined();
      expect(rates.usd).toBeDefined();
      expect(rates.eur).toBeDefined();
    });
  });

  describe('saveRatesForBatch', () => {
    it('should save exchange rates for batch', async () => {
      mockRepository.create.mockImplementation((data) => data);
      mockRepository.save.mockResolvedValue([]);

      await service.saveRatesForBatch('batch-1');

      expect(mockRepository.create).toHaveBeenCalled();
      expect(mockRepository.save).toHaveBeenCalled();
    });
  });

  describe('getRatesForBatch', () => {
    it('should return rates for batch', async () => {
      const mockRates = [
        { currencyCode: 'USD', rate: 1 },
        { currencyCode: 'EUR', rate: 0.92 },
      ];
      mockRepository.find.mockResolvedValue(mockRates);

      const result = await service.getRatesForBatch('batch-1');

      expect(result).toEqual(mockRates);
      expect(mockRepository.find).toHaveBeenCalledWith({
        where: { uploadBatchId: 'batch-1' },
      });
    });
  });

  describe('convertPrice', () => {
    it('should convert price to multiple currencies', () => {
      const rates = [
        { currencyCode: 'USD', rate: 1 },
        { currencyCode: 'EUR', rate: 0.92 },
      ] as ExchangeRate[];

      const result = service.convertPrice(100, rates);

      expect(result.USD).toBe(100);
      expect(result.EUR).toBe(92);
    });
  });
});
