import { useCallback } from 'react';
import { useDropzone } from 'react-dropzone';
import UploadCloud from '../assets/icons/upload-cloud.svg?react';
import { cn } from '../lib/utils';
import FileText from '../assets/icons/file-text.svg?react';

interface FileDropzoneProps {
  onFileSelect: (file: File) => void;
  setError: (msg: string | null) => void;
  disabled?: boolean;
}

export function FileDropzone({
  onFileSelect,
  setError,
  disabled,
}: FileDropzoneProps) {
  const onDrop = useCallback(
    (acceptedFiles: File[]) => {
      const file = acceptedFiles[0];
      if (file) {
        setError(null);
        onFileSelect(file);
      }
    },
    [onFileSelect, setError],
  );

  const onDropRejected = useCallback(() => {
    setError('Only CSV files up to 50MB are allowed');
  }, [setError]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    onDropRejected,
    multiple: false,
    disabled,
    accept: {
      'text/csv': ['.csv'],
      'application/vnd.ms-excel': ['.csv'],
    },
    maxSize: 50000 * 1024 * 1024,
  });

  return (
    <div
      {...getRootProps()}
      className={cn(
        'group relative flex w-full cursor-pointer flex-col items-center justify-center overflow-hidden rounded-2xl border border-dashed border-zinc-300 bg-white py-20 transition-all duration-500 ease-out',
        'hover:border-zinc-400 hover:bg-zinc-50/50 hover:shadow-2xl hover:shadow-zinc-200/50',
        isDragActive &&
          'scale-[1.02] border-blue-500 bg-blue-50/50 ring-4 ring-blue-500/10',
        disabled && 'pointer-events-none opacity-60',
      )}
    >
      <input {...getInputProps()} />

      {/* Decorative background blur blobs */}
      <div className="absolute -top-20 -left-20 h-64 w-64 rounded-full bg-gradient-to-br from-purple-50 to-blue-50 opacity-0 blur-3xl transition-opacity duration-700 group-hover:opacity-100" />
      <div className="absolute -right-20 -bottom-20 h-64 w-64 rounded-full bg-gradient-to-tl from-blue-50 to-indigo-50 opacity-0 blur-3xl transition-opacity duration-700 group-hover:opacity-100" />

      <div className="relative z-10 flex flex-col items-center gap-6">
        {/* Animated Icon Container */}
        <div
          className={cn(
            'flex h-20 w-20 items-center justify-center rounded-2xl bg-zinc-50 shadow-sm ring-1 ring-zinc-100 transition-all duration-500',
            'group-hover:-translate-y-2 group-hover:bg-white group-hover:shadow-xl group-hover:ring-zinc-200',
            isDragActive && 'scale-110 bg-blue-500 text-white ring-blue-400',
          )}
        >
          <UploadCloud
            className={cn(
              'h-8 w-8 text-zinc-400 transition-all duration-500',
              'group-hover:scale-110 group-hover:text-zinc-900',
              isDragActive && 'text-white',
            )}
          />
        </div>

        {/* Typography */}
        <div className="space-y-2 text-center transition-all duration-300 group-hover:-translate-y-1">
          <div className="text-lg font-semibold text-zinc-900">
            {isDragActive ? 'Drop your CSV now' : 'Upload your data'}
          </div>
          <div className="flex items-center justify-center gap-1.5 text-sm text-zinc-500">
            <span className="font-medium text-zinc-700 underline underline-offset-4 transition-colors group-hover:text-blue-600 group-hover:decoration-blue-600">
              Click to browse
            </span>
            <span>or drag and drop</span>
          </div>
        </div>

        {/* Footer Metadata */}
        <div className="flex items-center gap-3 rounded-full border border-zinc-100 bg-zinc-50/80 px-4 py-1.5 transition-all duration-300 group-hover:border-zinc-200 group-hover:bg-white">
          <FileText className="h-3.5 w-3.5 text-zinc-400" />
          <span className="text-xs font-medium text-zinc-500">
            CSV Supported
          </span>
          <span className="h-3 w-px bg-zinc-200" />
          <span className="text-xs text-zinc-400">Max 50MB</span>
        </div>
      </div>
    </div>
  );
}
