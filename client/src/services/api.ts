import network from './network';
import { API_ENDPOINTS } from './constants';
import type {
  UploadBatch,
  ProductsResponse,
  ProductFilters,
  BatchesResponse,
} from '../types/index.js';

const API_BASE = import.meta.env.VITE_API_BASE_URL || '/api';

// Auth APIs
export async function signup(): Promise<{ token: string; userId: string }> {
  const response = await network.post<{ token: string; userId: string }>({
    url: API_ENDPOINTS.AUTH_SIGNUP,
  });
  return response.data;
}

export async function login(
  userId: string,
): Promise<{ token: string; userId: string }> {
  const response = await network.post<{ token: string; userId: string }>({
    url: API_ENDPOINTS.AUTH_LOGIN,
    body: { userId },
  });
  return response.data;
}

// Upload APIs
export async function uploadFile(file: File): Promise<UploadBatch> {
  const formData = new FormData();
  formData.append('file', file);

  // Don't set Content-Type manually - Axios will set it with correct boundary
  const response = await network.post<UploadBatch>({
    url: API_ENDPOINTS.UPLOAD,
    body: formData,
  });
  return response.data;
}


export async function fetchBatches(): Promise<BatchesResponse> {
  const response = await network.get<BatchesResponse>({
    url: API_ENDPOINTS.UPLOAD_BATCHES,
  });
  return response.data;
}

// Products APIs
export async function fetchProducts(
  filters: ProductFilters,
): Promise<ProductsResponse> {
  const params: Record<string, string> = {
    page: String(filters.page),
    limit: String(filters.limit),
    sortBy: filters.sortBy,
    sortOrder: filters.sortOrder,
  };

  if (filters.filterName) params.filterName = filters.filterName;
  if (filters.priceMin !== undefined) params.priceMin = String(filters.priceMin);
  if (filters.priceMax !== undefined) params.priceMax = String(filters.priceMax);
  if (filters.expirationFrom) params.expirationFrom = filters.expirationFrom;
  if (filters.expirationTo) params.expirationTo = filters.expirationTo;
  if (filters.batchId) params.batchId = filters.batchId;

  const response = await network.get<ProductsResponse>({
    url: API_ENDPOINTS.PRODUCTS,
    params,
  });
  return response.data;
}

// SSE for progress tracking
export function createProgressEventSource(batchId: string): EventSource {
  return new EventSource(`${API_BASE}${API_ENDPOINTS.UPLOAD_PROGRESS(batchId)}`);
}
