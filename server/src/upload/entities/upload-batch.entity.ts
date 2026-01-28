import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  OneToMany,
  ManyToOne,
  JoinColumn,
} from 'typeorm';

export enum BatchStatus {
  PENDING = 'pending',
  PROCESSING = 'processing',
  COMPLETED = 'completed',
  FAILED = 'failed',
}

@Entity('upload_batches')
export class UploadBatch {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ length: 255 })
  fileName: string;

  @Column({
    type: 'enum',
    enum: BatchStatus,
    default: BatchStatus.PENDING,
  })
  status: BatchStatus;

  @Column({ default: 0 })
  totalRows: number;

  @Column({ default: 0 })
  processedRows: number;

  @Column({ type: 'text', nullable: true })
  errorMessage: string | null;

  @Column({ type: 'uuid', nullable: true })
  userId: string | null;

  @ManyToOne('User', { nullable: true, onDelete: 'SET NULL' })
  @JoinColumn({ name: 'userId' })
  user: unknown;

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;

  @OneToMany('Product', 'uploadBatch')
  products: unknown[];

  @OneToMany('ExchangeRate', 'uploadBatch')
  exchangeRates: unknown[];
}
