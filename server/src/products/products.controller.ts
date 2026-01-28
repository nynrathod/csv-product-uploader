import { Controller, Get, Query, Request } from '@nestjs/common';
import { ProductsService, PaginatedResponse } from './products.service.js';
import { GetProductsDto } from './dto/get-products.dto.js';

@Controller('products')
export class ProductsController {
  constructor(private readonly productsService: ProductsService) {}

  @Get()
  async findAll(
    @Query() query: GetProductsDto,
    @Request() req,
  ): Promise<PaginatedResponse> {
    // If user is authenticated (which they should be via global guard), filter by their userId
    if (req.user && req.user.userId) {
      query.userId = req.user.userId;
    }
    return this.productsService.findAll(query);
  }
}
