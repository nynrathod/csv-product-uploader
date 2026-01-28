import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  ManyToOne,
  JoinColumn,
} from 'typeorm';

@Entity('products')
export class Product {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ length: 500 })
  name: string;

  @Column({ type: 'decimal', precision: 10, scale: 2 })
  price: number;

  @Column({ type: 'date' })
  expirationDate: Date;

  @Column({ type: 'uuid' })
  uploadBatchId: string;

  @Column({ type: 'uuid', nullable: true })
  userId: string | null;

  @ManyToOne('UploadBatch', 'products', { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'uploadBatchId' })
  uploadBatch: unknown;

  @ManyToOne('User', { nullable: true, onDelete: 'SET NULL' })
  @JoinColumn({ name: 'userId' })
  user: unknown;

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
