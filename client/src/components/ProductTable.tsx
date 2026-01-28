import { useState } from 'react';
import type { Product, ProductFilters, PaginationInfo } from '../types';
import { Button } from './ui/Button';
import { Select } from './ui/Select';
import { TableSkeleton } from './common/TableSkeleton';
import { cn } from '../lib/utils';

// SVG Imports
import ChevronUp from '../assets/icons/chevron-up.svg?react';
import ChevronDown from '../assets/icons/chevron-down.svg?react';
import ChevronLeft from '../assets/icons/chevron-left.svg?react';
import ChevronRight from '../assets/icons/chevron-right.svg?react';
import ChevronsLeft from '../assets/icons/chevrons-left.svg?react';
import ChevronsRight from '../assets/icons/chevrons-right.svg?react';

// Reusable class constants
const TH_CLASS = 'h-10 px-4 align-middle font-medium text-zinc-500';
const TD_CLASS = 'p-4 align-middle';
const TR_CLASS = 'border-b border-zinc-100 transition-colors hover:bg-zinc-50/50';

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

const PAGE_SIZE_OPTIONS = [
  { value: 10, label: '10' },
  { value: 20, label: '20' },
  { value: 30, label: '30' },
  { value: 50, label: '50' },
  { value: 100, label: '100' },
];

export function ProductTable({
  products,
  loading,
  filters,
  pagination,
  onPageChange,
  onSort,
  onLimitChange,
}: ProductTableProps) {
  const [showSymbols, setShowSymbols] = useState(true);

  const formatCurrency = (amount: number, currency: string) => {
    const symbols: Record<string, string> = {
      USD: '$',
      EUR: '€',
      GBP: '£',
      JPY: '¥',
      AUD: 'A$',
    };
    return showSymbols
      ? `${symbols[currency] || currency}${amount.toFixed(2)}`
      : `${amount.toFixed(2)} ${currency}`;
  };

  const formatDate = (date: string) =>
    new Date(date).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric',
    });

  const handleSort = (column: ProductFilters['sortBy']) => {
    const newOrder =
      filters.sortBy === column && filters.sortOrder === 'ASC' ? 'DESC' : 'ASC';
    onSort(column, newOrder);
  };

  // Column header component
  const ColumnHeader = ({
    column,
    title,
    className,
  }: {
    column?: ProductFilters['sortBy'];
    title: string;
    className?: string;
  }) => {
    if (!column) {
      return (
        <div className={cn('h-8 bg-transparent px-2 text-left', className)}>
          {title}
        </div>
      );
    }

    const isSorted = filters.sortBy === column;
    const isAsc = filters.sortOrder === 'ASC';

    // Pre-compute sort icon
    const sortIcon = isSorted ? (
      isAsc ? (
        <ChevronUp className="h-3.5 w-3.5 text-zinc-900" />
      ) : (
        <ChevronDown className="h-3.5 w-3.5 text-zinc-900" />
      )
    ) : (
      <div className="flex flex-col opacity-0 group-hover:opacity-50">
        <ChevronUp className="h-3 w-3 -mb-1" />
        <ChevronDown className="h-3 w-3" />
      </div>
    );

    return (
      <div
        className={cn(
          'flex h-8 cursor-pointer items-center space-x-2 px-2 text-left align-middle font-medium text-zinc-500 transition-colors select-none hover:text-zinc-900 group',
          isSorted && 'text-zinc-900',
          className,
        )}
        onClick={() => handleSort(column)}
      >
        <span>{title}</span>
        {sortIcon}
      </div>
    );
  };

  // Loading skeleton column widths
  const skeletonColumns = [320, 80, 120, 200];

  // Pre-compute pagination values
  const start = Math.min((pagination.page - 1) * filters.limit + 1, pagination.total);
  const end = Math.min(pagination.page * filters.limit, pagination.total);
  const isFirstPage = pagination.page === 1;
  const isLastPage = pagination.page === pagination.totalPages;

  // Table body content
  const tableBody = loading ? (
    <TableSkeleton rows={15} columns={skeletonColumns} />
  ) : products.length === 0 ? (
    <tr>
      <td colSpan={4} className="h-24 text-center text-zinc-500">
        No results.
      </td>
    </tr>
  ) : (
    products.map((product) => (
      <tr key={product.id} className={`group ${TR_CLASS} last:border-0`}>
        <td
          className={`${TD_CLASS} max-w-[200px] truncate font-medium text-zinc-900`}
          title={product.name}
        >
          {product.name}
        </td>
        <td className={`${TD_CLASS} text-zinc-500`}>
          ${product.price.toFixed(2)}
        </td>
        <td className={`${TD_CLASS} text-zinc-500`}>
          {formatDate(product.expirationDate)}
        </td>
        <td className={`${TD_CLASS} text-right`}>
          <div className="flex items-center justify-end gap-3 font-mono text-xs text-zinc-500">
            {Object.entries(product.exchangeRates)
              .slice(0, 4)
              .map(([curr, val]) => (
                <span
                  key={curr}
                  title={curr}
                  className="cursor-help decoration-zinc-300 underline-offset-4 hover:underline"
                >
                  {formatCurrency(val, curr)}
                </span>
              ))}
          </div>
        </td>
      </tr>
    ))
  );

  // Mobile footer
  const mobileFooter = (
    <div className="sticky bottom-0 z-20 flex flex-row items-center justify-between gap-2 rounded-b-md border-t border-zinc-200 bg-white px-4 py-4 shadow-[0_-1px_2px_rgba(0,0,0,0.05)] sm:hidden">
      <div className="flex items-center">
        <Select
          value={filters.limit}
          onChange={(v) => onLimitChange(Number(v))}
          options={PAGE_SIZE_OPTIONS}
          className="h-9 w-[60px]"
        />
      </div>

      <div className="flex-1 text-center text-sm text-zinc-700">
        {start}-{end} of {pagination.total}
      </div>

      <div className="flex items-center gap-1">
        <button
          onClick={() => onPageChange(1)}
          disabled={isFirstPage}
          className="flex h-9 w-9 items-center justify-center rounded-md border border-zinc-200 bg-white text-xs font-medium text-zinc-900 shadow-sm transition-colors hover:bg-zinc-100 disabled:pointer-events-none disabled:opacity-50"
        >
          <ChevronsLeft className="h-3.5 w-3.5" />
        </button>
        <button
          onClick={() => onPageChange(pagination.page - 1)}
          disabled={isFirstPage}
          className="flex h-9 w-9 items-center justify-center rounded-md border border-zinc-200 bg-white text-xs font-medium text-zinc-900 shadow-sm transition-colors hover:bg-zinc-100 disabled:pointer-events-none disabled:opacity-50"
        >
          <ChevronLeft className="h-3.5 w-3.5" />
        </button>
        <button
          onClick={() => onPageChange(pagination.page + 1)}
          disabled={isLastPage}
          className="flex h-9 w-9 items-center justify-center rounded-md border border-zinc-200 bg-white text-xs font-medium text-zinc-900 shadow-sm transition-colors hover:bg-zinc-100 disabled:pointer-events-none disabled:opacity-50"
        >
          <ChevronRight className="h-3.5 w-3.5" />
        </button>
        <button
          onClick={() => onPageChange(pagination.totalPages)}
          disabled={isLastPage}
          className="flex h-9 w-9 items-center justify-center rounded-md border border-zinc-200 bg-white text-xs font-medium text-zinc-900 shadow-sm transition-colors hover:bg-zinc-100 disabled:pointer-events-none disabled:opacity-50"
        >
          <ChevronsRight className="h-3.5 w-3.5" />
        </button>
      </div>
    </div>
  );

  // Desktop footer
  const desktopFooter = (
    <div className="sticky bottom-0 z-20 hidden flex-row items-center justify-between gap-4 rounded-b-md border-t border-zinc-200 bg-white px-4 py-4 shadow-[0_-1px_2px_rgba(0,0,0,0.05)] sm:flex">
      <div className="text-sm text-zinc-500">
        {pagination.total} row{pagination.total !== 1 ? 's' : ''} available
      </div>

      <div className="flex items-center gap-6">
        <div className="flex items-center gap-2">
          <span className="text-sm text-zinc-500">Rows per page</span>
          <Select
            value={filters.limit}
            onChange={(v) => onLimitChange(Number(v))}
            options={PAGE_SIZE_OPTIONS}
            className="h-9 w-[70px]"
          />
        </div>

        <div className="text-sm text-zinc-700">
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
    <div className="flex h-full flex-col rounded-md border border-zinc-200 bg-white shadow-sm">
      <div className="relative w-full flex-1 overflow-auto rounded-t-md">
        <table className="w-full caption-bottom text-left text-sm">
          <thead className="sticky top-0 z-10 border-b border-zinc-200 bg-white shadow-sm">
            <tr className="border-b border-zinc-200 transition-colors hover:bg-zinc-50/50">
              <th className={`${TH_CLASS} w-[40%] min-w-[300px]`}>
                <ColumnHeader title="Name" column="name" />
              </th>
              <th className={`${TH_CLASS} w-[15%] min-w-[100px]`}>
                <ColumnHeader title="Price" column="price" />
              </th>
              <th className={`${TH_CLASS} w-[15%] min-w-[150px]`}>
                <ColumnHeader title="Date" column="expirationDate" />
              </th>
              <th className={`${TH_CLASS} w-[30%] text-right`}>
                <div className="flex items-center justify-end gap-2 text-xs font-normal">
                  <span className="text-zinc-400">Currency Mode:</span>
                  <button
                    onClick={() => setShowSymbols(!showSymbols)}
                    className="cursor-pointer font-medium text-zinc-900 underline-offset-2 hover:underline"
                  >
                    {showSymbols ? 'Symbols' : 'Codes'}
                  </button>
                </div>
              </th>
            </tr>
          </thead>

          <tbody className="bg-white [&_tr:last-child]:border-0">
            {tableBody}
          </tbody>
        </table>
      </div>

      {mobileFooter}
      {desktopFooter}
    </div>
  );
}
