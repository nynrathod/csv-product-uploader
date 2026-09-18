import { Injectable, UnauthorizedException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { JwtService } from '@nestjs/jwt';
import { Repository } from 'typeorm';
import { User } from './entities/user.entity.js';

@Injectable()
export class AuthService {
  constructor(
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
    private readonly jwtService: JwtService,
  ) {}

  async signup(): Promise<{ token: string; userId: string }> {
    const user = this.userRepository.create({});
    const savedUser = await this.userRepository.save(user);

    const token = this.generateToken(savedUser.id);
    return { token, userId: savedUser.id };
  }

  async login(userId: string): Promise<{ token: string; userId: string }> {
    const user = await this.userRepository.findOne({ where: { id: userId } });

    if (!user) {
      throw new UnauthorizedException('User not found');
    }

    const token = this.generateToken(user.id);
    return { token, userId: user.id };
  }

  async validateUser(userId: string): Promise<User | null> {
    return this.userRepository.findOne({ where: { id: userId } });
  }

  private generateToken(userId: string): string {
    return this.jwtService.sign({ sub: userId });
  }
}
