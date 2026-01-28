import { FilterPanel } from '../FilterPanel';
import { ProductTable } from '../ProductTable';
import { Button } from '../ui/Button';
import type { Product, ProductFilters, PaginationInfo, UploadBatch } from '../../types';

interface ProductsViewProps {
    products: Product[];
    loading: boolean;
    filters: ProductFilters;
    pagination: PaginationInfo;
    currentBatch: UploadBatch | undefined;
    onFilterChange: (filters: Partial<ProductFilters>) => void;
    onPageChange: (page: number) => void;
    onSort: (sortBy: ProductFilters['sortBy'], sortOrder: ProductFilters['sortOrder']) => void;
    onLimitChange: (limit: number) => void;
    onBackToBatches: () => void;
    onUploadNew: () => void;
}

export function ProductsView({
    products,
    loading,
    filters,
    pagination,
    currentBatch,
    onFilterChange,
    onPageChange,
    onSort,
    onLimitChange,
    onBackToBatches,
    onUploadNew,
}: ProductsViewProps) {
    return (
        <div className="animate-in fade-in flex h-full min-h-0 flex-col duration-300">
            {/* Header */}
            <div className="mb-4 flex-none">
                <div className="flex items-center justify-between">
                    <div className="space-y-1">
                        <div className="flex items-center gap-2 text-sm text-zinc-500">
                            <span
                                className="cursor-pointer transition-colors hover:text-zinc-900"
                                onClick={onBackToBatches}
                            >
                                Batches
                            </span>
                            <span className="text-zinc-300">/</span>
                            <span className="font-medium text-zinc-900">{currentBatch?.fileName}</span>
                        </div>
                        <h2 className="text-2xl font-bold tracking-tight">Products</h2>
                    </div>
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

            {/* Table */}
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
