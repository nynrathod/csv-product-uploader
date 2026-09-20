import type { UploadBatch } from '../types';
import { Button } from './ui/Button';
import { TableSkeleton } from './common/TableSkeleton';

// Reusable class constants
const TH_CLASS = 'h-12 px-4 align-middle font-medium text-zinc-500';
const TD_CLASS = 'p-4 align-middle';
const TR_CLASS = 'border-b border-zinc-100 transition-colors hover:bg-zinc-50/50';

interface BatchListProps {
  batches: UploadBatch[];
  loading: boolean;
  onViewProducts: (batchId: string) => void;
  onViewProgress: (batchId: string) => void;
  onUploadNew: () => void;
}

export function BatchList({
  batches,
  loading,
  onViewProducts,
  onViewProgress,
  onUploadNew,
}: BatchListProps) {
  const formatDate = (date: string) =>
    new Date(date).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });

  const getStatusBadge = (status: UploadBatch['status']) => {
    const styles: Record<UploadBatch['status'], string> = {
      pending: 'bg-yellow-100 text-yellow-800 hover:bg-yellow-100/80',
      processing: 'bg-blue-100 text-blue-800 hover:bg-blue-100/80',
      completed: 'bg-green-100 text-green-800 hover:bg-green-100/80',
      failed: 'bg-red-100 text-red-800 hover:bg-red-100/80',
    };

    return (
      <div
        className={`inline-flex items-center rounded-full border border-transparent px-2.5 py-0.5 text-xs font-semibold transition-colors ${styles[status]}`}
      >
        {status.charAt(0).toUpperCase() + status.slice(1)}
      </div>
    );
  };

  // Empty state
  if (batches.length === 0 && !loading) {
    return (
      <div className="flex h-[450px] shrink-0 items-center justify-center rounded-md border border-dashed border-zinc-200">
        <div className="mx-auto flex max-w-[420px] flex-col items-center justify-center text-center">
          <div className="flex h-20 w-20 items-center justify-center rounded-full bg-zinc-100">
            <svg className="h-10 w-10 text-zinc-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
            </svg>
          </div>
          <h3 className="mt-4 text-lg font-semibold">No uploads yet</h3>
          <p className="mt-2 mb-4 text-sm text-zinc-500">You haven't uploaded any product data yet.</p>
          <Button onClick={onUploadNew}>Upload CSV</Button>
        </div>
      </div>
    );
  }

  // Table headers config
  const headers = [
    { label: 'File Name', className: TH_CLASS },
    { label: 'Status', className: TH_CLASS },
    { label: 'Products', className: TH_CLASS },
    { label: 'Uploaded', className: TH_CLASS },
    { label: 'Actions', className: `${TH_CLASS} text-right` },
  ];

  // Loading skeleton column widths
  const skeletonColumns = [128, 80, 48, 144, 96];

  // Table body content
  const tableBody = loading ? (
    <TableSkeleton rows={3} columns={skeletonColumns} />
  ) : (
    batches.map((batch) => {
      // Pre-compute action button
      const actionButton =
        batch.status === 'completed' ? (
          <button
            onClick={() => onViewProducts(batch.id)}
            className="inline-flex h-8 cursor-pointer items-center justify-center rounded-md border border-zinc-200 bg-white px-2 text-xs font-medium shadow-sm transition-colors hover:bg-zinc-100 sm:px-3 sm:text-sm"
          >
            <span className="hidden sm:inline">View Products</span>
            <span className="sm:hidden">View</span>
          </button>
        ) : batch.status === 'processing' ? (
          <button
            onClick={() => onViewProgress(batch.id)}
            className="inline-flex h-8 cursor-pointer items-center justify-center rounded-md border border-blue-200 bg-blue-50 px-2 text-xs font-medium text-blue-700 shadow-sm transition-colors hover:bg-blue-100 sm:px-3"
          >
            <span className="mr-2 h-2 w-2 animate-pulse rounded-full bg-blue-500" />
            <span className="hidden sm:inline">View Progress</span>
            <span className="sm:hidden">Progress</span>
          </button>
        ) : null;

      // Product count display
      const productCount =
        batch.status === 'processing'
          ? `${batch.processedRows} / ${batch.totalRows}`
          : batch.totalRows;

      return (
        <tr key={batch.id} className={`group ${TR_CLASS}`}>
          <td className={`${TD_CLASS} font-medium`}>{batch.fileName}</td>
          <td className={TD_CLASS}>{getStatusBadge(batch.status)}</td>
          <td className={`${TD_CLASS} text-zinc-500`}>{productCount}</td>
          <td className={`${TD_CLASS} text-zinc-500`}>{formatDate(batch.createdAt)}</td>
          <td className={`${TD_CLASS} text-right`}>{actionButton}</td>
        </tr>
      );
    })
  );

  return (
    <div className="animate-in fade-in flex h-full flex-col space-y-4 duration-500">
      {/* Header */}
      <div className="flex flex-none items-center justify-between">
        <div className="space-y-1">
          <h2 className="text-2xl font-semibold tracking-tight">Dashboard</h2>
          <p className="text-sm text-zinc-500">Manage your product batch uploads</p>
        </div>
        <Button onClick={onUploadNew}>Upload New</Button>
      </div>

      {/* Table */}
      <div className="flex min-h-0 flex-1 flex-col rounded-md border border-zinc-200 bg-white shadow-sm">
        <div className="relative w-full flex-1 overflow-auto rounded-md">
          <table className="w-full caption-bottom text-left text-sm">
            <thead className="sticky top-0 z-10 border-b border-zinc-200 bg-white shadow-sm">
              <tr className="border-b border-zinc-200 transition-colors hover:bg-zinc-50/50">
                {headers.map((header, idx) => (
                  <th key={idx} className={header.className}>{header.label}</th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white [&_tr:last-child]:border-0">
              {tableBody}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
