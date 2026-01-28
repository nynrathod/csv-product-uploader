import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  ManyToOne,
  JoinColumn,
  Unique,
} from 'typeorm';
import { UploadBatch } from '../../upload/entities/upload-batch.entity';

@Entity('exchange_rates')
@Unique(['uploadBatchId', 'currencyCode'])
export class ExchangeRate {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ type: 'uuid' })
  uploadBatchId: string;

  @Column({ length: 3 })
  currencyCode: string;

  @Column({ type: 'decimal', precision: 18, scale: 6 })
  rate: number;

  @Column({ type: 'timestamp' })
  capturedAt: Date;

  @ManyToOne(() => UploadBatch, (batch) => batch.exchangeRates, {
    onDelete: 'CASCADE',
  })
  @JoinColumn({ name: 'uploadBatchId' })
  uploadBatch: UploadBatch;

  @CreateDateColumn()
  createdAt: Date;
}
