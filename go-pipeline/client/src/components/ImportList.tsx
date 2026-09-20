import type { ImportJob } from '../types';
import { Button } from './ui/Button';
import { TableSkeleton } from './common/TableSkeleton';

const TH = 'h-12 px-4 align-middle font-medium text-zinc-500';
const TD = 'p-4 align-middle';
const TR = 'border-b border-zinc-100 transition-colors hover:bg-zinc-50/50';

interface ImportListProps {
  imports: ImportJob[];
  loading: boolean;
  onViewProducts: (id: string) => void;
  onViewProgress: (id: string) => void;
  onViewReport: (id: string) => void;
  onUploadNew: () => void;
}

export function ImportList({
  imports,
  loading,
  onViewProducts,
  onViewProgress,
  onViewReport,
  onUploadNew,
}: ImportListProps) {
  const formatDate = (date: string) =>
    new Date(date).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });

  const getStatusBadge = (status: ImportJob['status']) => {
    const styles: Record<ImportJob['status'], string> = {
      pending: 'bg-yellow-100 text-yellow-800',
      processing: 'bg-blue-100 text-blue-800',
      completed: 'bg-green-100 text-green-800',
      failed: 'bg-red-100 text-red-800',
    };
    return (
      <div
        className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold ${styles[status]}`}
      >
        {status === 'processing' && (
          <span className="mr-1.5 h-1.5 w-1.5 animate-pulse rounded-full bg-blue-500" />
        )}
        {status.charAt(0).toUpperCase() + status.slice(1)}
      </div>
    );
  };

  if (imports.length === 0 && !loading) {
    return (
      <div className="flex h-[450px] shrink-0 items-center justify-center rounded-md border border-dashed border-zinc-200">
        <div className="mx-auto flex max-w-[420px] flex-col items-center justify-center text-center">
          <div className="flex h-20 w-20 items-center justify-center rounded-full bg-zinc-100">
            <svg
              className="h-10 w-10 text-zinc-400"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={1}
                d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"
              />
            </svg>
          </div>
          <h3 className="mt-4 text-lg font-semibold text-zinc-900">
            No imports yet
          </h3>
          <p className="mt-2 mb-4 text-sm text-zinc-500">
            Upload a product CSV to start an import.
          </p>
          <Button onClick={onUploadNew}>Upload CSV</Button>
        </div>
      </div>
    );
  }

  const headers = [
    { label: 'File', className: TH },
    { label: 'Status', className: TH },
    { label: 'Rows', className: TH },
    { label: 'Processed', className: TH },
    { label: 'Uploaded', className: TH },
    { label: 'Actions', className: `${TH} text-right` },
  ];

  const tableBody = loading ? (
    <TableSkeleton rows={3} columns={[140, 90, 90, 90, 140, 130]} />
  ) : (
    imports.map((job) => {
      const actionButton =
        job.status === 'completed' ? (
          <div className="flex justify-end gap-2">
            <button
              onClick={() => onViewProducts(job.id)}
              className="inline-flex h-8 cursor-pointer items-center rounded-md border border-zinc-200 bg-white px-2 text-xs font-medium shadow-sm transition-colors hover:bg-zinc-100 sm:px-3"
            >
              View Products
            </button>
            <button
              onClick={() => onViewReport(job.id)}
              className="inline-flex h-8 cursor-pointer items-center rounded-md border border-zinc-200 bg-white px-2 text-xs font-medium text-zinc-600 shadow-sm transition-colors hover:bg-zinc-100 sm:px-3"
            >
              Report
            </button>
          </div>
        ) : job.status === 'processing' || job.status === 'pending' ? (
          <button
            onClick={() => onViewProgress(job.id)}
            className="inline-flex h-8 cursor-pointer items-center rounded-md border border-blue-200 bg-blue-50 px-2 text-xs font-medium text-blue-700 shadow-sm transition-colors hover:bg-blue-100 sm:px-3"
          >
            <span className="mr-2 h-2 w-2 animate-pulse rounded-full bg-blue-500" />
            View Progress
          </button>
        ) : (
          <button
            onClick={() => onViewReport(job.id)}
            className="inline-flex h-8 cursor-pointer items-center rounded-md border border-zinc-200 bg-white px-2 text-xs font-medium text-zinc-600 shadow-sm transition-colors hover:bg-zinc-100 sm:px-3"
          >
            Details
          </button>
        );

      return (
        <tr key={job.id} className={TR}>
          <td
            className={`${TD} max-w-[220px] truncate font-medium text-zinc-900`}
            title={job.fileName}
          >
            {job.fileName}
          </td>
          <td className={TD}>
            <span title={job.lastError ?? undefined}>
              {getStatusBadge(job.status)}
            </span>
          </td>
          <td className={`${TD} text-zinc-600 tabular-nums`}>
            {job.validRows.toLocaleString()} / {job.totalRows.toLocaleString()}
            {job.invalidRows > 0 && (
              <span className="ml-1 text-xs text-amber-600">
                ({job.invalidRows.toLocaleString()} invalid)
              </span>
            )}
          </td>
          <td className={`${TD} text-zinc-600 tabular-nums`}>
            {job.publishedRows > 0
              ? `${job.processedRows.toLocaleString()} / ${job.publishedRows.toLocaleString()}`
              : '—'}
          </td>
          <td className={`${TD} text-zinc-500`}>{formatDate(job.createdAt)}</td>
          <td className={`${TD} text-right`}>{actionButton}</td>
        </tr>
      );
    })
  );

  return (
    <div className="animate-in fade-in flex h-full flex-col space-y-4 duration-500">
      <div className="flex flex-none items-center justify-between">
        <div className="space-y-1">
          <h2 className="text-2xl font-semibold tracking-tight text-zinc-900">
            Imports
          </h2>
          <p className="text-sm text-zinc-500">
            Product imports across the pipeline
          </p>
        </div>
        <Button onClick={onUploadNew}>Upload New</Button>
      </div>

      <div className="flex min-h-0 flex-1 flex-col rounded-md border border-zinc-200 bg-white shadow-sm">
        <div className="relative w-full flex-1 overflow-auto rounded-md">
          <table className="w-full caption-bottom text-left text-sm">
            <thead className="sticky top-0 z-10 border-b border-zinc-200 bg-white shadow-sm">
              <tr>
                {headers.map((h, i) => (
                  <th key={i} className={h.className}>
                    {h.label}
                  </th>
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
