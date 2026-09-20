import type {
  ImportJob,
  ImportReport,
  ProductFilters,
  ProductsResponse,
} from '../types';
import { API_BASE } from './constants';

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, init);
  if (!res.ok) {
    const body = (await res.json().catch(() => null)) as {
      error?: string;
    } | null;
    throw new Error(body?.error ?? `Request failed with status ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export function uploadCsv(file: File): Promise<ImportJob> {
  const form = new FormData();
  form.append('file', file);
  return request('/api/v1/imports', { method: 'POST', body: form });
}

export function listImports(): Promise<ImportJob[]> {
  return request('/api/v1/imports');
}

export function getImport(id: string): Promise<ImportJob> {
  return request(`/api/v1/imports/${id}`);
}

export function getImportReport(id: string): Promise<ImportReport> {
  return request(`/api/v1/imports/${id}/report`);
}

export function getProducts(
  jobId: string,
  filters: ProductFilters,
  page: number,
): Promise<ProductsResponse> {
  const params = new URLSearchParams({
    page: String(page),
    limit: String(filters.limit),
    sortBy: filters.sortBy,
    sortOrder: filters.sortOrder,
  });
  if (filters.filterName) params.set('filterName', filters.filterName);
  return request(`/api/v1/imports/${jobId}/products?${params.toString()}`);
}
