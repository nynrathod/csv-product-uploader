import { useState, useEffect } from "react";
import type { ProductFilters } from "../types";

interface FilterPanelProps {
  filters: ProductFilters;
  onFilterChange: (updates: Partial<ProductFilters>) => void;
}

export function FilterPanel({ filters, onFilterChange }: FilterPanelProps) {
  const [searchValue, setSearchValue] = useState(filters.filterName || "");

  useEffect(() => {
    const timer = setTimeout(() => {
      if (searchValue !== (filters.filterName || "")) {
        onFilterChange({ filterName: searchValue || undefined });
      }
    }, 300);
    return () => clearTimeout(timer);
  }, [searchValue, filters.filterName, onFilterChange]);

  return (
    <div className="relative">
      <input
        type="text"
        value={searchValue}
        onChange={(e) => setSearchValue(e.target.value)}
        placeholder="Filter products..."
        className="flex h-8 w-[150px] lg:w-[250px] rounded-md border border-zinc-200 bg-transparent px-3 py-1 text-sm shadow-sm transition-colors file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-zinc-500 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-zinc-950 disabled:cursor-not-allowed disabled:opacity-50"
      />
    </div>
  );
}
