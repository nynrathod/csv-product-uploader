import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { ExchangeRate } from './entities/exchange-rate.entity.js';

interface RateData {
  [currency: string]: number;
}

const SUPPORTED_CURRENCIES = ['usd', 'eur', 'gbp', 'jpy', 'aud'];

const FALLBACK_RATES: { [key: string]: number } = {
  usd: 1.0,
  eur: 0.92,
  gbp: 0.79,
  jpy: 149.5,
  aud: 1.53,
};

@Injectable()
export class ExchangeRatesService {
  private cachedRates: RateData | null = null;
  private cacheTimestamp: number = 0;
  private readonly CACHE_TTL = 24 * 60 * 60 * 1000;

  constructor(
    @InjectRepository(ExchangeRate)
    private readonly exchangeRateRepository: Repository<ExchangeRate>,
  ) { }

  /**
   * Fetches USD-based rates from external API with caching and fallback.
   */
  async fetchRates(): Promise<RateData> {
    if (this.cachedRates && Date.now() - this.cacheTimestamp < this.CACHE_TTL) {
      return this.cachedRates;
    }

    try {
      const response = await fetch(
        'https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json',
      );

      if (!response.ok) {
        throw new Error(`API returned ${response.status}`);
      }

      const data = await response.json();
      const rates: RateData = {};

      for (const currency of SUPPORTED_CURRENCIES) {
        if (data.usd && data.usd[currency] !== undefined) {
          rates[currency] = data.usd[currency];
        } else {
          rates[currency] = FALLBACK_RATES[currency];
        }
      }

      this.cachedRates = rates;
      this.cacheTimestamp = Date.now();

      return rates;
    } catch {
      console.warn('Failed to fetch exchange rates, using fallback rates');
      return FALLBACK_RATES;
    }
  }

  /**
   * Captures and stores exchange rates snapshot for a specific upload batch.
   */
  async saveRatesForBatch(batchId: string): Promise<ExchangeRate[]> {
    const rates = await this.fetchRates();
    const capturedAt = new Date();

    const exchangeRates: ExchangeRate[] = [];

    for (const [currency, rate] of Object.entries(rates)) {
      const exchangeRate = this.exchangeRateRepository.create({
        uploadBatchId: batchId,
        currencyCode: currency.toUpperCase(),
        rate,
        capturedAt,
      });
      exchangeRates.push(exchangeRate);
    }

    return this.exchangeRateRepository.save(exchangeRates);
  }

  async getRatesForBatch(batchId: string): Promise<ExchangeRate[]> {
    return this.exchangeRateRepository.find({
      where: { uploadBatchId: batchId },
    });
  }

  convertPrice(
    price: number,
    rates: ExchangeRate[],
  ): { [currency: string]: number } {
    const converted: { [currency: string]: number } = {};

    for (const rate of rates) {
      converted[rate.currencyCode] = Math.round(price * rate.rate * 100) / 100;
    }

    return converted;
  }
}
