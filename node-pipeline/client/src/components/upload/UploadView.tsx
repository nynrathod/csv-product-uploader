import { FileDropzone } from '../FileDropzone';
import { Button } from '../ui/Button';
import ChevronLeft from '../../assets/icons/chevron-left.svg?react';
import FileText from '../../assets/icons/file-text.svg?react';
import XClose from '../../assets/icons/x-close.svg?react';
import AlertCircle from '../../assets/icons/alert-circle.svg?react';

interface UploadViewProps {
    pendingFile: File | null;
    isUploading: boolean;
    error: string | null;
    onFileSelect: (file: File) => void;
    onClearFile: () => void;
    onStartUpload: () => void;
    onCancel: () => void;
    setError: (error: string | null) => void;
}

export function UploadView({
    pendingFile,
    isUploading,
    error,
    onFileSelect,
    onClearFile,
    onStartUpload,
    onCancel,
    setError,
}: UploadViewProps) {
    // Back button with chevron icon
    const backButton = (
        <div
            className="flex w-fit cursor-pointer items-center gap-2 text-sm text-zinc-500 transition-colors hover:text-zinc-900"
            onClick={onCancel}
        >
            <ChevronLeft className="h-4 w-4" />
            Back to dashboard
        </div>
    );

    // File preview card when file is selected
    const filePreview = pendingFile ? (
        <div className="animate-in fade-in zoom-in-95 flex flex-col items-center p-8 duration-200">
            <div className="mb-6 flex w-full items-center gap-4 rounded-lg border border-zinc-200 bg-zinc-50 p-4">
                <div className="flex h-10 w-10 items-center justify-center rounded-full border border-zinc-200 bg-white shadow-sm">
                    <FileText className="h-5 w-5 text-zinc-600" />
                </div>
                <div className="min-w-0 flex-1">
                    <p className="truncate text-sm font-semibold text-zinc-900">{pendingFile.name}</p>
                    <p className="text-xs text-zinc-500">{(pendingFile.size / 1024).toFixed(1)} KB</p>
                </div>
                <button
                    onClick={onClearFile}
                    className="p-2 text-zinc-400 transition-colors hover:text-red-500"
                    title="Remove file"
                >
                    <XClose className="h-5 w-5" />
                </button>
            </div>

            <div className="flex w-full flex-col gap-3 sm:w-auto sm:flex-row">
                <Button variant="outline" onClick={onClearFile} className="w-full sm:w-auto">
                    Choose Different File
                </Button>
                <Button onClick={onStartUpload} className="w-full min-w-[140px] sm:w-auto">
                    Start Processing
                </Button>
            </div>
        </div>
    ) : null;

    // Dropzone when no file selected
    const dropzone = !pendingFile ? (
        <FileDropzone onFileSelect={onFileSelect} setError={setError} disabled={isUploading} />
    ) : null;

    // Error message
    const errorMessage = error ? (
        <div className="animate-in slide-in-from-top-2 fade-in m-4 flex items-center gap-2 rounded border border-red-100 bg-red-50 p-3 text-sm text-red-600 duration-200">
            <AlertCircle className="h-4 w-4" />
            {error}
        </div>
    ) : null;

    return (
        <div className="animate-in fade-in slide-in-from-bottom-4 mx-auto w-full max-w-xl pt-10 duration-500">
            <div className="space-y-6">
                {backButton}

                <div className="space-y-1">
                    <h2 className="text-2xl font-semibold tracking-tight">Upload CSV</h2>
                    <p className="text-sm text-zinc-500">
                        Import your product data. We'll handle the exchange rates.
                    </p>
                </div>

                <div className=" transition-colors">
                    {dropzone}
                    {filePreview}
                    {errorMessage}
                </div>
            </div>
        </div>
    );
}
