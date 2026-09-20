import type { Product, ProductFilters, PaginationInfo } from '../types';
import { Button } from './ui/Button';
import { Select } from './ui/Select';
import { TableSkeleton } from './common/TableSkeleton';
import { cn } from '../lib/utils';

import ChevronUp from '../assets/icons/chevron-up.svg?react';
import ChevronDown from '../assets/icons/chevron-down.svg?react';
import ChevronLeft from '../assets/icons/chevron-left.svg?react';
import ChevronRight from '../assets/icons/chevron-right.svg?react';
import ChevronsLeft from '../assets/icons/chevrons-left.svg?react';
import ChevronsRight from '../assets/icons/chevrons-right.svg?react';

const TH = 'h-10 px-4 align-middle font-medium text-zinc-500';
const TD = 'p-4 align-middle';

const PAGE_SIZE_OPTIONS = [
  { value: 10, label: '10' },
  { value: 20, label: '20' },
  { value: 50, label: '50' },
  { value: 100, label: '100' },
];

const formatPrice = (cents: number) =>
  (cents / 100).toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });

const formatDate = (date: string | null) =>
  date
    ? new Date(date).toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'long',
        day: 'numeric',
      })
    : '—';

interface ProductTableProps {
  products: Product[];
  loading: boolean;
  filters: ProductFilters;
  pagination: PaginationInfo;
  onPageChange: (page: number) => void;
  onSort: (
    sortBy: ProductFilters['sortBy'],
    sortOrder: ProductFilters['sortOrder'],
  ) => void;
  onLimitChange: (limit: number) => void;
}

export function ProductTable({
  products,
  loading,
  filters,
  pagination,
  onPageChange,
  onSort,
  onLimitChange,
}: ProductTableProps) {
  const handleSort = (column: ProductFilters['sortBy']) => {
    const newOrder =
      filters.sortBy === column && filters.sortOrder === 'ASC' ? 'DESC' : 'ASC';
    onSort(column, newOrder);
  };

  const SortHeader = ({
    column,
    title,
    className,
  }: {
    column: ProductFilters['sortBy'];
    title: string;
    className?: string;
  }) => {
    const isSorted = filters.sortBy === column;
    return (
      <div
        className={cn(
          'flex h-8 cursor-pointer items-center gap-2 px-2 text-left align-middle font-medium transition-colors select-none hover:text-zinc-900',
          isSorted ? 'text-zinc-900' : 'text-zinc-500',
          className,
        )}
        onClick={() => handleSort(column)}
      >
        <span>{title}</span>
        {isSorted ? (
          filters.sortOrder === 'ASC' ? (
            <ChevronUp className="h-3.5 w-3.5" />
          ) : (
            <ChevronDown className="h-3.5 w-3.5" />
          )
        ) : (
          <div className="flex flex-col opacity-0 group-hover:opacity-40">
            <ChevronUp className="-mb-1 h-3 w-3" />
            <ChevronDown className="h-3 w-3" />
          </div>
        )}
      </div>
    );
  };

  const start =
    pagination.total === 0
      ? 0
      : Math.min((pagination.page - 1) * filters.limit + 1, pagination.total);
  const end = Math.min(pagination.page * filters.limit, pagination.total);
  const isFirstPage = pagination.page === 1;
  const isLastPage = pagination.page === pagination.totalPages;

  const tableBody = loading ? (
    <TableSkeleton rows={15} columns={[280, 90, 70, 120, 110, 120]} />
  ) : products.length === 0 ? (
    <tr>
      <td colSpan={6} className="h-24 text-center text-zinc-500">
        No products match.
      </td>
    </tr>
  ) : (
    products.map((p) => (
      <tr
        key={`${p.merchantId}:${p.productId}`}
        className="group border-b border-zinc-100 transition-colors last:border-0 hover:bg-zinc-50/50"
      >
        <td
          className={`${TD} max-w-[260px] truncate font-medium text-zinc-900`}
          title={p.name}
        >
          {p.name}
        </td>
        <td className={`${TD} text-right text-zinc-700 tabular-nums`}>
          {formatPrice(p.priceCents)}
        </td>
        <td className={TD}>
          <span className="inline-flex items-center rounded-full bg-zinc-100 px-2 py-0.5 text-[11px] font-semibold text-zinc-600">
            {p.currency}
          </span>
        </td>
        <td className={`${TD} text-zinc-500`}>
          {formatDate(p.expirationDate)}
        </td>
        <td className={`${TD} font-mono text-xs text-zinc-500`}>
          {p.merchantId}
        </td>
        <td className={`${TD} font-mono text-xs text-zinc-400`}>
          {p.productId}
        </td>
      </tr>
    ))
  );

  const footer = (
    <div className="sticky bottom-0 z-20 hidden flex-row items-center justify-between gap-4 rounded-b-md border-t border-zinc-200 bg-white px-4 py-4 shadow-[0_-1px_2px_rgba(0,0,0,0.05)] sm:flex">
      <div className="text-sm text-zinc-500">
        {pagination.total.toLocaleString()} product
        {pagination.total !== 1 ? 's' : ''} · {start.toLocaleString()}–
        {end.toLocaleString()}
      </div>
      <div className="flex items-center gap-6">
        <div className="flex items-center gap-2">
          <span className="text-sm text-zinc-500">Rows per page</span>
          <Select
            value={filters.limit}
            onChange={(v) => onLimitChange(Number(v))}
            options={PAGE_SIZE_OPTIONS}
          />
        </div>
        <div className="text-sm text-zinc-700 tabular-nums">
          Page {pagination.page} of {pagination.totalPages || 1}
        </div>
        <div className="flex items-center gap-1">
          <Button
            variant="outline"
            size="sm"
            onClick={() => onPageChange(1)}
            disabled={isFirstPage}
            className="h-8 w-8 p-0"
          >
            <ChevronsLeft className="h-3.5 w-3.5" />
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => onPageChange(pagination.page - 1)}
            disabled={isFirstPage}
            className="h-8 w-8 p-0"
          >
            <ChevronLeft className="h-3.5 w-3.5" />
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => onPageChange(pagination.page + 1)}
            disabled={isLastPage}
            className="h-8 w-8 p-0"
          >
            <ChevronRight className="h-3.5 w-3.5" />
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => onPageChange(pagination.totalPages)}
            disabled={isLastPage}
            className="h-8 w-8 p-0"
          >
            <ChevronsRight className="h-3.5 w-3.5" />
          </Button>
        </div>
      </div>
    </div>
  );

  return (
    <div className="flex min-h-0 flex-1 flex-col rounded-md border border-zinc-200 bg-white shadow-sm">
      <div className="relative w-full flex-1 overflow-auto rounded-md">
        <table className="w-full caption-bottom text-left text-sm">
          <thead className="sticky top-0 z-10 border-b border-zinc-200 bg-white shadow-sm">
            <tr>
              <th className={TH}>
                <SortHeader column="name" title="Name" />
              </th>
              <th className={TH}>
                <div className="flex justify-end">
                  <SortHeader column="price" title="Price" />
                </div>
              </th>
              <th className={TH}>Currency</th>
              <th className={TH}>
                <SortHeader column="expiration" title="Expiration" />
              </th>
              <th className={TH}>Merchant</th>
              <th className={TH}>Product ID</th>
            </tr>
          </thead>
          <tbody className="bg-white">{tableBody}</tbody>
        </table>
      </div>
      {footer}
    </div>
  );
}
