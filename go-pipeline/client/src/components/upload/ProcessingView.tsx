import { ImportProgress } from '../ImportProgress';
import { Button } from '../ui/Button';
import type { ImportJob } from '../../types';

interface ProcessingViewProps {
  job: ImportJob | null;
  isComplete: boolean;
  onBack: () => void;
}

export function ProcessingView({
  job,
  isComplete,
  onBack,
}: ProcessingViewProps) {
  const failed = isComplete && job?.status === 'failed';
  const completed = isComplete && job?.status === 'completed';

  return (
    <div className="animate-in fade-in slide-in-from-bottom-4 mx-auto w-full max-w-xl pt-10 duration-500">
      <div className="space-y-8">
        <div className="space-y-2 text-center">
          <h3 className="text-lg font-medium text-zinc-900">
            {completed
              ? 'Import complete'
              : failed
                ? 'Import failed'
                : 'Processing file...'}
          </h3>
          <p className="truncate text-sm text-zinc-500">{job?.fileName}</p>
        </div>

        <ImportProgress job={job} />

        {completed && (
          <div className="rounded-lg border border-green-100 bg-green-50 p-3 text-center text-sm font-medium text-green-700">
            Catalog confirmed every event — opening products...
          </div>
        )}

        {failed && (
          <div className="rounded-lg border border-red-100 bg-red-50 p-3 text-sm text-red-600">
            <span className="font-medium">Import failed.</span>{' '}
            {job?.lastError ?? 'Unknown cause.'}
          </div>
        )}

        <div className="text-center">
          <button
            onClick={onBack}
            className="cursor-pointer text-sm text-zinc-500 underline underline-offset-4 transition-colors hover:text-zinc-900"
          >
            ← Back to imports
          </button>
          {!isComplete && (
            <p className="mt-2 text-xs text-zinc-400">
              Processing continues in the background
            </p>
          )}
          {failed && (
            <div className="mt-4 text-center">
              <Button onClick={onBack} variant="outline">
                Back to imports
              </Button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
