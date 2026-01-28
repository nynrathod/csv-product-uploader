export interface UploadBatch {
  id: string;
  fileName: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  totalRows: number;
  processedRows: number;
  createdAt: string;
  updatedAt: string;
  errorMessage?: string;
}

export interface BatchesResponse {
  success: boolean;
  data: UploadBatch[];
}

export interface ProgressEvent {
  id: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  totalRows: number;
  processedRows: number;
  percentage: number;
  errorMessage?: string;
  updatedAt: string;
}

export interface Product {
  id: string;
  name: string;
  price: number;
  expirationDate: string;
  uploadDate: string;
  exchangeRates: { [currency: string]: number };
}

export interface ProductsResponse {
  success: boolean;
  data: Product[];
  pagination: {
    page: number;
    limit: number;
    total: number;
    totalPages: number;
  };
}

export interface ProductFilters {
  page: number;
  limit: number;
  sortBy: 'name' | 'price' | 'expirationDate';
  sortOrder: 'ASC' | 'DESC';
  filterName?: string;
  priceMin?: number;
  priceMax?: number;
  expirationFrom?: string;
  expirationTo?: string;
  batchId?: string;
}

export interface PaginationInfo {
  page: number;
  limit: number;
  total: number;
  totalPages: number;
}

