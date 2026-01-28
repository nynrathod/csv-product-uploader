import { Processor, Process, OnQueueFailed } from '@nestjs/bull';
import type { Job } from 'bull';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import * as fs from 'fs';
import csvParser from 'csv-parser';
import {
  UploadBatch,
  BatchStatus,
} from '../upload/entities/upload-batch.entity.js';
import { Product } from '../products/entities/product.entity.js';
import { UploadService } from '../upload/upload.service.js';
import { ExchangeRatesService } from '../exchange-rates/exchange-rates.service.js';

interface CsvRow {
  name: string;
  price: string;
  expiration: string;
}

interface ProcessedRow {
  name: string;
  price: number;
  expirationDate: Date;
  uploadBatchId: string;
  userId: string | null;
}

const BATCH_SIZE = 500;

@Processor('csv-processing')
export class CsvProcessor {
  constructor(
    @InjectRepository(UploadBatch)
    private readonly uploadBatchRepository: Repository<UploadBatch>,
    @InjectRepository(Product)
    private readonly productRepository: Repository<Product>,
    private readonly uploadService: UploadService,
    private readonly exchangeRatesService: ExchangeRatesService,
  ) {}

  @Process('process-csv')
  async handleProcessCsv(
    job: Job<{ batchId: string; filePath: string; userId?: string }>,
  ) {
    const { batchId, filePath, userId } = job.data;
    console.log(`[CSV Processor] Starting job for batch: ${batchId}`);

    try {
      await this.uploadBatchRepository.update(batchId, {
        status: BatchStatus.PROCESSING,
      });

      await this.exchangeRatesService.saveRatesForBatch(batchId);

      const rows = await this.parseCSV(filePath);
      console.log(`[CSV Processor] Parsed ${rows.length} rows`);

      const validRows = this.validateRows(rows, batchId, userId || null);
      console.log(`[CSV Processor] Validated ${validRows.length} rows`);

      await this.uploadBatchRepository.update(batchId, {
        totalRows: validRows.length,
      });

      this.emitProgress(batchId, 0, validRows.length, BatchStatus.PROCESSING);

      for (let i = 0; i < validRows.length; i += BATCH_SIZE) {
        const batch = validRows.slice(i, i + BATCH_SIZE);
        await this.productRepository.save(batch);

        const processed = Math.min(i + BATCH_SIZE, validRows.length);
        await this.uploadBatchRepository.update(batchId, {
          processedRows: processed,
        });

        this.emitProgress(
          batchId,
          processed,
          validRows.length,
          BatchStatus.PROCESSING,
        );
        await job.progress(Math.round((processed / validRows.length) * 100));
      }

      await this.uploadBatchRepository.update(batchId, {
        status: BatchStatus.COMPLETED,
        processedRows: validRows.length,
      });

      this.emitProgress(
        batchId,
        validRows.length,
        validRows.length,
        BatchStatus.COMPLETED,
      );
      this.cleanupFile(filePath);
      console.log(`[CSV Processor] Completed job for batch: ${batchId}`);

      return { success: true, processedRows: validRows.length };
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown error';
      console.error(`[CSV Processor] Failed:`, error);

      await this.uploadBatchRepository.update(batchId, {
        status: BatchStatus.FAILED,
        errorMessage,
      });

      this.emitProgress(batchId, 0, 0, BatchStatus.FAILED, errorMessage);
      this.cleanupFile(filePath);
      throw error;
    }
  }

  @OnQueueFailed()
  handleFailed(job: Job, error: Error) {
    console.error(`[CSV Processor] Job ${job.id} failed:`, error.message);
  }

  private parseCSV(filePath: string): Promise<CsvRow[]> {
    return new Promise((resolve, reject) => {
      const rows: CsvRow[] = [];

      fs.createReadStream(filePath)
        .pipe(csvParser({ separator: ';' }))
        .on('data', (row: CsvRow) => rows.push(row))
        .on('end', () => resolve(rows))
        .on('error', reject);
    });
  }

  private validateRows(
    rows: CsvRow[],
    batchId: string,
    userId: string | null,
  ): ProcessedRow[] {
    const validRows: ProcessedRow[] = [];

    for (const row of rows) {
      const name = row.name?.trim();
      const priceStr = row.price?.replace('$', '').replace(',', '.').trim();
      const expirationStr = row.expiration?.trim();

      if (!name || !priceStr || !expirationStr) continue;

      const price = parseFloat(priceStr);
      if (isNaN(price) || price < 0) continue;

      const expirationDate = this.parseDate(expirationStr);
      if (!expirationDate || isNaN(expirationDate.getTime())) continue;

      validRows.push({
        name: name.substring(0, 500),
        price,
        expirationDate,
        uploadBatchId: batchId,
        userId,
      });
    }

    return validRows;
  }

  private parseDate(dateStr: string): Date | null {
    const parts = dateStr.split('/');
    if (parts.length !== 3) return null;

    const [month, day, year] = parts.map((p) => parseInt(p, 10));
    if (isNaN(month) || isNaN(day) || isNaN(year)) return null;

    const fullYear = year < 100 ? 2000 + year : year;
    return new Date(fullYear, month - 1, day);
  }

  private emitProgress(
    batchId: string,
    processed: number,
    total: number,
    status: BatchStatus,
    errorMessage?: string,
  ) {
    const percentage = total > 0 ? Math.round((processed / total) * 100) : 0;
    this.uploadService.emitProgress(batchId, {
      id: batchId,
      status,
      totalRows: total,
      processedRows: processed,
      percentage,
      errorMessage,
      updatedAt: new Date().toISOString(),
    });
  }

  private cleanupFile(filePath: string) {
    try {
      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
      }
    } catch {
      console.warn(`[CSV Processor] Failed to cleanup file: ${filePath}`);
    }
  }
}
