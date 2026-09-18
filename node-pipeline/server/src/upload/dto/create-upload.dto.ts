import { IsNotEmpty } from 'class-validator';

export class CreateUploadDto {
  @IsNotEmpty()
  file: Express.Multer.File;
}
