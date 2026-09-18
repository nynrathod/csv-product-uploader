import { IsNotEmpty, IsUUID } from 'class-validator';

export class LoginDto {
  @IsNotEmpty()
  @IsUUID()
  userId: string;
}
