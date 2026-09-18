import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { UploadService } from './upload.service';
import { UploadBatch, BatchStatus } from './entities/upload-batch.entity';
import { getQueueToken } from '@nestjs/bull';

describe('UploadService', () => {
  let service: UploadService;

  const mockRepository = {
    create: jest.fn(),
    save: jest.fn(),
    findOne: jest.fn(),
    update: jest.fn(),
  };

  const mockQueue = {
    add: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        UploadService,
        { provide: getRepositoryToken(UploadBatch), useValue: mockRepository },
        { provide: getQueueToken('csv-processing'), useValue: mockQueue },
      ],
    }).compile();

    service = module.get<UploadService>(UploadService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('createBatch', () => {
    it('should create a batch and enqueue job', async () => {
      const mockBatch = {
        id: 'test-uuid',
        fileName: 'test.csv',
        status: BatchStatus.PENDING,
        totalRows: 0,
        processedRows: 0,
      };

      mockRepository.create.mockReturnValue(mockBatch);
      mockRepository.save.mockResolvedValue(mockBatch);
      mockQueue.add.mockResolvedValue({});

      const result = await service.createBatch('test.csv', '/path/to/file.csv');

      expect(result.fileName).toBe('test.csv');
      expect(result.status).toBe(BatchStatus.PENDING);
      expect(mockQueue.add).toHaveBeenCalledWith('process-csv', {
        batchId: 'test-uuid',
        filePath: '/path/to/file.csv',
        userId: null,
      }, {
        attempts: 3,
        backoff: 1000,
      });
    });
  });

  describe('getBatch', () => {
    it('should return batch by id', async () => {
      const mockBatch = { id: 'test-uuid', fileName: 'test.csv' };
      mockRepository.findOne.mockResolvedValue(mockBatch);

      const result = await service.getBatch('test-uuid');

      expect(result).toEqual(mockBatch);
      expect(mockRepository.findOne).toHaveBeenCalledWith({
        where: { id: 'test-uuid' },
      });
    });

    it('should return null if batch not found', async () => {
      mockRepository.findOne.mockResolvedValue(null);

      const result = await service.getBatch('non-existent');

      expect(result).toBeNull();
    });
  });

  describe('progress emitters', () => {
    it('should register and unregister emitters', () => {
      const emitter = service.registerProgressEmitter('batch-1');
      expect(emitter).toBeDefined();

      service.unregisterProgressEmitter('batch-1');
    });

    it('should emit progress events', () => {
      const emitter = service.registerProgressEmitter('batch-1');
      const handler = jest.fn();
      emitter.on('progress', handler);

      service.emitProgress('batch-1', { percentage: 50 });

      expect(handler).toHaveBeenCalledWith({ percentage: 50 });
    });
  });
});
