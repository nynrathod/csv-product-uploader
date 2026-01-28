import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { InjectQueue } from '@nestjs/bull';
import { Repository, DeepPartial } from 'typeorm';
import type { Queue } from 'bull';
import { EventEmitter } from 'events';
import { UploadBatch, BatchStatus } from './entities/upload-batch.entity.js';

@Injectable()
export class UploadService {
  private progressEmitters = new Map<string, EventEmitter>();

  constructor(
    @InjectRepository(UploadBatch)
    private readonly uploadBatchRepository: Repository<UploadBatch>,
    @InjectQueue('csv-processing')
    private readonly csvQueue: Queue,
  ) {}

  /**
   * Creates a new batch record and queues it for asynchronous CSV processing.
   */
  async createBatch(
    fileName: string,
    filePath: string,
    userId?: string,
  ): Promise<UploadBatch> {
    const batch = this.uploadBatchRepository.create({
      fileName,
      status: BatchStatus.PENDING,
      totalRows: 0,
      processedRows: 0,
      userId: userId || null,
    });

    const savedBatch = await this.uploadBatchRepository.save(batch);

    try {
      await this.csvQueue.add(
        'process-csv',
        { batchId: savedBatch.id, filePath, userId: userId || null },
        { attempts: 3, backoff: 1000 },
      );
    } catch (error) {
      console.error('Failed to add job to queue:', error);
      await this.uploadBatchRepository.update(savedBatch.id, {
        status: BatchStatus.FAILED,
        errorMessage: 'Failed to start processing',
      });
    }

    return savedBatch;
  }

  async getBatch(id: string): Promise<UploadBatch | null> {
    return this.uploadBatchRepository.findOne({ where: { id } });
  }

  async updateBatch(
    id: string,
    updates: DeepPartial<UploadBatch>,
  ): Promise<UploadBatch | null> {
    await this.uploadBatchRepository.update(
      id,
      updates as Record<string, unknown>,
    );
    return this.getBatch(id);
  }

  async getBatchesByUserId(userId: string): Promise<UploadBatch[]> {
    return this.uploadBatchRepository.find({
      where: { userId },
      order: { createdAt: 'DESC' },
    });
  }

  registerProgressEmitter(batchId: string): EventEmitter {
    const emitter = new EventEmitter();
    this.progressEmitters.set(batchId, emitter);
    return emitter;
  }

  unregisterProgressEmitter(batchId: string): void {
    this.progressEmitters.delete(batchId);
  }

  emitProgress(batchId: string, data: object): void {
    const emitter = this.progressEmitters.get(batchId);
    if (emitter) {
      emitter.emit('progress', data);
    }
  }
}
