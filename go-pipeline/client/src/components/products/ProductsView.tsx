import { useEffect, useState } from 'react';
import { FilterPanel } from '../FilterPanel';
import { ProductTable } from '../ProductTable';
import { Button } from '../ui/Button';
import { getImport } from '../../services/api';
import type {
  ImportJob,
  PaginationInfo,
  Product,
  ProductFilters,
} from '../../types';

interface ProductsViewProps {
  jobId: string;
  products: Product[];
  pagination: PaginationInfo;
  loading: boolean;
  filters: ProductFilters;
  onFilterChange: (filters: Partial<ProductFilters>) => void;
  onPageChange: (page: number) => void;
  onSort: (
    sortBy: ProductFilters['sortBy'],
    sortOrder: ProductFilters['sortOrder'],
  ) => void;
  onLimitChange: (limit: number) => void;
  onBack: () => void;
  onUploadNew: () => void;
}

export function ProductsView({
  jobId,
  products,
  pagination,
  loading,
  filters,
  onFilterChange,
  onPageChange,
  onSort,
  onLimitChange,
  onBack,
  onUploadNew,
}: ProductsViewProps) {
  const [job, setJob] = useState<ImportJob | null>(null);

  useEffect(() => {
    let cancelled = false;
    getImport(jobId)
      .then((j) => {
        if (!cancelled) setJob(j);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [jobId]);

  const summary = [
    { label: 'Total', value: job?.totalRows },
    { label: 'Valid', value: job?.validRows },
    { label: 'Invalid', value: job?.invalidRows },
    { label: 'Processed', value: job?.processedRows },
    { label: 'Dead-lettered', value: job?.deadRows },
  ];

  return (
    <div className="animate-in fade-in flex h-full min-h-0 flex-col duration-300">
      <div className="mb-4 flex-none">
        <div className="flex items-center justify-between">
          <div className="space-y-1">
            <div className="flex items-center gap-2 text-sm text-zinc-500">
              <span
                className="cursor-pointer transition-colors hover:text-zinc-900"
                onClick={onBack}
              >
                Imports
              </span>
              <span className="text-zinc-300">/</span>
              <span className="font-medium text-zinc-900">
                {job?.fileName ?? '…'}
              </span>
            </div>
            <h2 className="text-2xl font-bold tracking-tight text-zinc-900">
              Products
            </h2>
          </div>
        </div>

        <div className="mt-4 grid grid-cols-2 gap-3 sm:grid-cols-5">
          {summary.map((s) => (
            <div
              key={s.label}
              className="rounded-lg border border-zinc-100 bg-white px-3 py-2"
            >
              <div className="text-[11px] font-medium tracking-wide text-zinc-400 uppercase">
                {s.label}
              </div>
              <div className="text-sm font-semibold text-zinc-900 tabular-nums">
                {s.value === undefined ? '…' : s.value.toLocaleString()}
              </div>
            </div>
          ))}
        </div>

        <div className="mt-4 flex items-center justify-between space-x-2">
          <div className="flex flex-1 items-center space-x-2">
            <FilterPanel filters={filters} onFilterChange={onFilterChange} />
          </div>
          <Button onClick={onUploadNew} className="h-8 px-3 text-xs">
            Upload New
          </Button>
        </div>
      </div>

      <div className="min-h-0 flex-1">
        <ProductTable
          products={products}
          loading={loading}
          filters={filters}
          pagination={pagination}
          onPageChange={onPageChange}
          onSort={onSort}
          onLimitChange={onLimitChange}
        />
      </div>
    </div>
  );
}
