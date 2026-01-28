import { useState, useCallback, useEffect } from 'react';
import type { ProductsResponse, ProductFilters, Product } from '../types';
import { fetchProducts } from '../services/api';
import { authService } from '../services/auth';

const defaultFilters: ProductFilters = {
  page: 1,
  limit: 50,
  sortBy: 'name',
  sortOrder: 'ASC',
};

export function useProductsData(batchId?: string) {
  const [products, setProducts] = useState<Product[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFilters] = useState<ProductFilters>(defaultFilters);
  const [pagination, setPagination] = useState({
    page: 1,
    limit: 50,
    total: 0,
    totalPages: 0,
  });

  const isAuthenticated = Boolean(authService.getToken());

  const load = useCallback(
    async (currentFilters: ProductFilters, currentBatchId?: string) => {
      // Don't fetch if not authenticated
      if (!isAuthenticated) {
        console.log('[useProductsData] Not authenticated, skipping fetch');
        setProducts([]);
        setLoading(false);
        return;
      }

      const timestamp = new Date().toISOString().split('T')[1];
      console.log(
        `%c[useProductsData ${timestamp}] LOAD CALLED - batchId: ${currentBatchId || 'ALL'}`,
        'color: blue; font-weight: bold',
      );
      console.log(
        `[useProductsData] Current filters:`,
        JSON.stringify(currentFilters),
      );

      setLoading(true);
      setError(null);

      try {
        const filtersWithBatch = { ...currentFilters, batchId: currentBatchId };
        console.log(
          `[useProductsData ${timestamp}] Calling API with:`,
          JSON.stringify(filtersWithBatch),
        );

        const response: ProductsResponse =
          await fetchProducts(filtersWithBatch);

        console.log(
          `%c[useProductsData ${timestamp}] API RESPONSE - ${response.data.length} products for batch: ${currentBatchId}`,
          response.data.length > 0
            ? 'color: green; font-weight: bold'
            : 'color: red; font-weight: bold',
        );
        console.log(
          `[useProductsData ${timestamp}] Pagination:`,
          JSON.stringify(response.pagination),
        );

        // Log first product if exists
        if (response.data.length > 0) {
          console.log(
            `[useProductsData ${timestamp}] First product:`,
            response.data[0]?.name,
          );
        }

        setProducts(response.data);
        setPagination(response.pagination);

        console.log(
          `%c[useProductsData ${timestamp}] STATE UPDATED - products set to ${response.data.length} items`,
          'color: purple',
        );
      } catch (err) {
        const message =
          err instanceof Error ? err.message : 'Failed to load products';
        console.error(
          `%c[useProductsData ${timestamp}] ERROR: ${message}`,
          'color: red; font-weight: bold',
        );
        setError(message);
      } finally {
        setLoading(false);
        console.log(`[useProductsData ${timestamp}] Loading set to false`);
      }
    },
    [isAuthenticated],
  );

  // Load when batchId changes or on initial mount
  useEffect(() => {
    const timestamp = new Date().toISOString().split('T')[1];
    console.log(
      `%c[useProductsData ${timestamp}] EFFECT TRIGGERED - batchId changed to: ${batchId || 'undefined'}`,
      'color: orange; font-weight: bold',
    );

    if (!isAuthenticated) {
      console.log(
        `[useProductsData ${timestamp}] Not authenticated, clearing products`,
      );
      setProducts([]);
      return;
    }

    // Always load - with batchId filter if provided, or all products if not
    console.log(
      `[useProductsData ${timestamp}] Calling load() with filters:`,
      JSON.stringify(filters),
    );
    load(filters, batchId);
  }, [batchId, isAuthenticated]); // Intentionally not including 'load' and 'filters' to avoid loops

  const updateFilters = useCallback(
    (updates: Partial<ProductFilters>) => {
      if (!isAuthenticated) return;
      const newFilters = { ...filters, ...updates, page: updates.page ?? 1 };
      setFilters(newFilters);
      load(newFilters, batchId);
    },
    [filters, load, isAuthenticated, batchId],
  );

  const changePage = useCallback(
    (page: number) => {
      if (!isAuthenticated) return;
      const newFilters = { ...filters, page };
      setFilters(newFilters);
      load(newFilters, batchId);
    },
    [filters, load, isAuthenticated, batchId],
  );

  const updateSort = useCallback(
    (
      sortBy: ProductFilters['sortBy'],
      sortOrder: ProductFilters['sortOrder'],
    ) => {
      if (!isAuthenticated) return;
      const newFilters = { ...filters, sortBy, sortOrder, page: 1 };
      setFilters(newFilters);
      load(newFilters, batchId);
    },
    [filters, load, isAuthenticated, batchId],
  );

  const refresh = useCallback(() => {
    if (isAuthenticated) {
      load(filters, batchId);
    }
  }, [isAuthenticated, batchId, filters, load]);

  return {
    products,
    loading,
    error,
    filters,
    pagination,
    updateFilters,
    changePage,
    updateSort,
    refresh,
  };
}
