export interface ImportJob {
  id: string;
  fileName: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  totalRows: number;
  validRows: number;
  invalidRows: number;
  publishedRows: number;
  processedRows: number;
  retriedRows: number;
  deadRows: number;
  lastError?: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface ImportReport {
  jobId: string;
  fileName: string;
  status: string;
  rows: { total: number; valid: number; invalid: number };
  events: {
    published: number;
    processed: number;
    retried: number;
    dead: number;
  };
  reconciliation: {
    published: number;
    confirmed: number;
    unconfirmed: number;
    state: string;
  };
  generatedAt: string;
}

export type ProductSortBy = 'name' | 'price' | 'expiration';
export type SortOrder = 'ASC' | 'DESC';

export interface ProductFilters {
  filterName?: string;
  sortBy: ProductSortBy;
  sortOrder: SortOrder;
  limit: number;
}

export interface Product {
  merchantId: string;
  productId: string;
  name: string;
  priceCents: number;
  currency: string;
  expirationDate: string | null;
}

export interface PaginationInfo {
  page: number;
  limit: number;
  total: number;
  totalPages: number;
}

export interface ProductsResponse {
  data: Product[];
  pagination: PaginationInfo;
}
