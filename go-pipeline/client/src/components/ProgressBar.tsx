import type { ProgressEvent } from '../types';
import { cn } from '../lib/utils';
import Loader from '../assets/icons/loader.svg?react';
import Check from '../assets/icons/check.svg?react';
import XClose from '../assets/icons/x-close.svg?react';
import AlertTriangle from '../assets/icons/alert-triangle.svg?react';

interface ProgressBarProps {
  progress: ProgressEvent | null;
}

export function ProgressBar({ progress }: ProgressBarProps) {
  // Initial loading state
  if (!progress) {
    return (
      <div className="w-full max-w-md mx-auto p-6 bg-white rounded-xl border border-zinc-200 shadow-sm animate-in fade-in zoom-in-95 duration-300">
        <div className="flex flex-col items-center gap-4 text-center">
          <div className="w-8 h-8 rounded-full flex items-center justify-center">
            <Loader className="w-8 h-8 text-zinc-900 animate-spin" />
          </div>
          <div className="space-y-1">
            <h3 className="text-sm font-semibold text-zinc-900">Initializing Upload...</h3>
            <p className="text-xs text-zinc-500">Preparing connection</p>
          </div>
        </div>
      </div>
    );
  }

  const percentage = progress.percentage ?? 0;
  const isComplete = progress.status === 'completed';
  const isFailed = progress.status === 'failed';
  const isProcessing = !isComplete && !isFailed;

  return (
    <div className="w-full max-w-xl mx-auto p-6 bg-white rounded-xl border border-zinc-200 shadow-sm animate-in fade-in slide-in-from-bottom-2 duration-500">
      <div className="space-y-6">
        {/* Header Section */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div
              className={cn(
                'w-10 h-10 rounded-full flex items-center justify-center border transition-colors duration-500',
                isComplete
                  ? 'bg-green-50 border-green-200 text-green-600'
                  : isFailed
                    ? 'bg-red-50 border-red-200 text-red-600'
                    : 'bg-zinc-50 border-zinc-100 text-zinc-900',
              )}
            >
              {isComplete ? (
                <Check className="w-5 h-5 animate-in zoom-in duration-300" />
              ) : isFailed ? (
                <XClose className="w-5 h-5 animate-in zoom-in duration-300" />
              ) : (
                <Loader className="w-5 h-5 animate-spin" />
              )}
            </div>
            <div>
              <h3 className="text-base font-semibold text-zinc-900">
                {isComplete
                  ? 'Processing Complete'
                  : isFailed
                    ? 'Processing Failed'
                    : 'Processing File'}
              </h3>
              <p className="text-xs text-zinc-500 font-medium">
                {isComplete
                  ? 'All rows imported successfully'
                  : isFailed
                    ? 'An error occurred'
                    : 'Please keep this window open'}
              </p>
            </div>
          </div>
          <div className="text-right">
            <div className="text-2xl font-bold text-zinc-900 tabular-nums tracking-tight">
              {percentage}%
            </div>
          </div>
        </div>

        {/* Bar Section */}
        <div className="relative h-3 w-full bg-zinc-100 rounded-full overflow-hidden">
          <div
            className={cn(
              'absolute top-0 left-0 h-full transition-all duration-300 ease-out will-change-transform',
              isComplete
                ? 'bg-green-500'
                : isFailed
                  ? 'bg-red-500'
                  : 'bg-zinc-900',
            )}
            style={{ width: `${percentage}%` }}
          />
          {/* Shimmer effect while processing */}
          {isProcessing && (
            <div className="absolute top-0 left-0 h-full w-full animate-shimmer bg-gradient-to-r from-transparent via-white/20 to-transparent -translate-x-full" />
          )}
        </div>

        {/* Stats Section */}
        <div className="grid grid-cols-2 gap-4 pt-2">
          <div className="p-3 bg-zinc-50 rounded-lg border border-zinc-100">
            <p className="text-xs text-zinc-500 font-medium uppercase tracking-wider">
              Processed Rows
            </p>
            <p className="text-sm font-semibold text-zinc-900 mt-1 tabular-nums">
              {progress.processedRows.toLocaleString()}{' '}
              <span className="text-zinc-400 font-normal">
                / {progress.totalRows.toLocaleString()}
              </span>
            </p>
          </div>
          <div className="p-3 bg-zinc-50 rounded-lg border border-zinc-100">
            <p className="text-xs text-zinc-500 font-medium uppercase tracking-wider">
              Status
            </p>
            <div className="flex items-center gap-2 mt-1">
              <span
                className={cn(
                  'w-2 h-2 rounded-full',
                  isComplete
                    ? 'bg-green-500'
                    : isFailed
                      ? 'bg-red-500'
                      : 'bg-blue-500 animate-pulse',
                )}
              />
              <p className="text-sm font-medium text-zinc-900 capitalize">
                {progress.status}
              </p>
            </div>
          </div>
        </div>

        {/* Error Message */}
        {isFailed && progress.errorMessage && (
          <div className="p-4 bg-red-50 text-red-700 text-sm rounded-lg border border-red-100 flex items-start gap-3 animate-in slide-in-from-top-1">
            <AlertTriangle className="w-5 h-5 flex-shrink-0 mt-0.5" />
            <span className="font-medium">{progress.errorMessage}</span>
          </div>
        )}
      </div>
    </div>
  );
}
