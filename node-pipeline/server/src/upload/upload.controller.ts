import {
  Controller,
  Post,
  Get,
  Param,
  UseInterceptors,
  UploadedFile,
  BadRequestException,
  NotFoundException,
  UnauthorizedException,
  ParseUUIDPipe,
  Sse,
  Request,
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { diskStorage } from 'multer';
import { extname, join } from 'path';
import { v4 as uuidv4 } from 'uuid';
import { Observable } from 'rxjs';
import { UploadService } from './upload.service.js';
import { Public } from '../auth/public.decorator.js';

const MAX_FILE_SIZE = 50 * 1024 * 1024;

interface MessageEvent {
  data: string | object;
  id?: string;
  type?: string;
  retry?: number;
}

@Controller('upload')
export class UploadController {
  constructor(private readonly uploadService: UploadService) {}

  @Get('batches')
  async getBatches(@Request() req) {
    const userId = req.user?.userId;
    if (!userId) {
      throw new UnauthorizedException();
    }
    const batches = await this.uploadService.getBatchesByUserId(userId);
    return { success: true, data: batches };
  }

  @Post()
  @UseInterceptors(
    FileInterceptor('file', {
      storage: diskStorage({
        destination: './uploads',
        filename: (_req, file, cb) => {
          const uniqueName = `${uuidv4()}${extname(file.originalname)}`;
          cb(null, uniqueName);
        },
      }),
      limits: { fileSize: MAX_FILE_SIZE },
      fileFilter: (_req, file, cb) => {
        const validMimeTypes = ['text/csv', 'application/vnd.ms-excel'];
        const validExtensions = ['.csv'];
        const ext = extname(file.originalname).toLowerCase();

        if (
          validExtensions.includes(ext) ||
          validMimeTypes.includes(file.mimetype)
        ) {
          cb(null, true);
        } else {
          cb(new BadRequestException('Only CSV files are allowed'), false);
        }
      },
    }),
  )
  async uploadFile(@UploadedFile() file: Express.Multer.File, @Request() req) {
    if (!file) {
      throw new BadRequestException('File is required');
    }

    const filePath = join(process.cwd(), file.path);
    const userId = req.user?.userId;
    const batch = await this.uploadService.createBatch(
      file.originalname,
      filePath,
      userId,
    );

    return {
      id: batch.id,
      fileName: batch.fileName,
      status: batch.status,
      totalRows: batch.totalRows,
      processedRows: batch.processedRows,
      createdAt: batch.createdAt,
    };
  }

  @Public()
  @Sse('progress/:batchId')
  streamProgress(
    @Param('batchId', ParseUUIDPipe) batchId: string,
  ): Observable<MessageEvent> {
    return new Observable<MessageEvent>((observer) => {
      console.log(`[SSE] Starting stream for batch: ${batchId}`);
      let isCompleted = false;

      const completeStream = () => {
        if (!isCompleted) {
          isCompleted = true;
          console.log(`[SSE] Completing stream for batch: ${batchId}`);
          this.uploadService.unregisterProgressEmitter(batchId);
          observer.complete(); // This closes the HTTP response
        }
      };

      const sendProgress = (data: object) => {
        if (!isCompleted) {
          observer.next({ data });
        }
      };

      // Send initial status
      (async () => {
        try {
          const batch = await this.uploadService.getBatch(batchId);
          if (!batch) {
            observer.error(new NotFoundException('Batch not found'));
            return;
          }

          const percentage =
            batch.totalRows > 0
              ? Math.round((batch.processedRows / batch.totalRows) * 100)
              : 0;

          console.log(`[SSE] Initial status: ${batch.status}, ${percentage}%`);

          sendProgress({
            id: batch.id,
            status: batch.status,
            totalRows: batch.totalRows,
            processedRows: batch.processedRows,
            percentage,
            updatedAt: batch.updatedAt,
            errorMessage: batch.errorMessage,
          });

          // If already done, complete immediately
          if (['completed', 'failed'].includes(batch.status)) {
            console.log(`[SSE] Batch already ${batch.status}, closing NOW`);
            completeStream();
            return;
          }
        } catch (err) {
          console.error(`[SSE] Error:`, err);
          observer.error(err);
        }
      })();

      // Register for progress updates
      const emitter = this.uploadService.registerProgressEmitter(batchId);

      let lastLoggedPercentage = -1;

      const handler = (data: any) => {
        // Throttle logs: only log every 10%
        if (
          data.percentage % 10 === 0 &&
          data.percentage !== lastLoggedPercentage
        ) {
          console.log(`[SSE] Progress: ${data.status}, ${data.percentage}%`);
          lastLoggedPercentage = data.percentage;
        }

        sendProgress(data);

        // Complete immediately when done
        if (['completed', 'failed'].includes(data.status)) {
          console.log(`[SSE] Work done, closing stream NOW`);
          completeStream();
        }
      };

      emitter.on('progress', handler);

      // Cleanup on client disconnect
      return () => {
        console.log(`[SSE] Client disconnected: ${batchId}`);
        emitter.off('progress', handler);
        this.uploadService.unregisterProgressEmitter(batchId);
      };
    });
  }
}
