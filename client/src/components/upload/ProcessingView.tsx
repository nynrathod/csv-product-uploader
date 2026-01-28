import { ProgressBar } from '../ProgressBar';
import { Button } from '../ui/Button';
import type { ProgressEvent } from '../../types';

interface ProcessingViewProps {
    progress: ProgressEvent | null;
    isFailed: boolean;
    onBack: () => void;
    onRetry: () => void;
}

export function ProcessingView({
    progress,
    isFailed,
    onBack,
    onRetry,
}: ProcessingViewProps) {
    // Failed state action button
    const failedAction = isFailed ? (
        <div className="text-center">
            <Button onClick={onRetry} variant="destructive">Try Again</Button>
        </div>
    ) : null;

    return (
        <div className="animate-in fade-in slide-in-from-bottom-4 mx-auto w-full max-w-xl pt-10 duration-500">
            <div className="space-y-8">
                <div className="space-y-2 text-center">
                    <h3 className="text-lg font-medium">Processing File...</h3>
                    <p className="text-sm text-zinc-500">This might take a few moments</p>
                </div>

                <ProgressBar progress={progress} />

                <div className="text-center">
                    <button
                        onClick={onBack}
                        className="cursor-pointer text-sm text-zinc-500 underline underline-offset-4 transition-colors hover:text-zinc-900"
                    >
                        ← Back to File upload
                    </button>
                    <p className="mt-2 text-xs text-zinc-400">
                        Processing will continue in the background
                    </p>
                </div>

                {failedAction}
            </div>
        </div>
    );
}
