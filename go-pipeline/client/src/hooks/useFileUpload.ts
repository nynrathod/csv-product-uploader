import { useState } from 'react';
import { uploadCsv } from '../services/api';

export function useFileUpload() {
  const [pendingFile, setPendingFile] = useState<File | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);

  const onFileSelect = (file: File) => {
    setError(null);
    setPendingFile(file);
  };

  const onClearFile = () => setPendingFile(null);

  const onStartUpload = async () => {
    if (!pendingFile || isUploading) return;
    setIsUploading(true);
    setError(null);
    try {
      const job = await uploadCsv(pendingFile);
      setActiveJobId(job.id);
      setPendingFile(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Upload failed');
    } finally {
      setIsUploading(false);
    }
  };

  const onCancel = () => setPendingFile(null);

  return {
    pendingFile,
    isUploading,
    error,
    activeJobId,
    onFileSelect,
    onClearFile,
    onStartUpload,
    onCancel,
    setError,
  };
}
