import { useCallback, useEffect, useState } from 'react';
import type { PaginationInfo, Product, ProductFilters } from '../types';
import { getProducts } from '../services/api';

const EMPTY_PAGINATION: PaginationInfo = {
  page: 1,
  limit: 20,
  total: 0,
  totalPages: 1,
};

export function useProductsData(
  jobId: string | null,
  filters: ProductFilters,
  page: number,
) {
  const [products, setProducts] = useState<Product[]>([]);
  const [pagination, setPagination] =
    useState<PaginationInfo>(EMPTY_PAGINATION);
  const [loading, setLoading] = useState(false);

  const fetchPage = useCallback(async () => {
    if (!jobId) {
      setProducts([]);
      setPagination(EMPTY_PAGINATION);
      return;
    }
    setLoading(true);
    try {
      const res = await getProducts(jobId, filters, page);
      setProducts(res.data);
      setPagination(res.pagination);
    } catch {
      setProducts([]);
      setPagination(EMPTY_PAGINATION);
    } finally {
      setLoading(false);
    }
  }, [
    jobId,
    page,
    filters.filterName,
    filters.sortBy,
    filters.sortOrder,
    filters.limit,
  ]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    fetchPage();
  }, [fetchPage]);

  // The projection trails the catalog worker: an empty first page for a
  // just-completed import means the read model is still catching up, so
  // poll briefly until rows appear.
  useEffect(() => {
    if (!jobId || pagination.total > 0) return;
    const t = setInterval(fetchPage, 2000);
    return () => clearInterval(t);
  }, [jobId, pagination.total, fetchPage]);

  return { products, pagination, loading };
}
