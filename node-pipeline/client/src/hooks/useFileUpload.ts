import { useState, useCallback } from 'react';
import type { UploadBatch } from '../types';
import { uploadFile } from '../services/api';

export function useFileUpload() {
  const [file, setFile] = useState<File | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [batch, setBatch] = useState<UploadBatch | null>(null);

  const upload = useCallback(async (selectedFile: File) => {
    setFile(selectedFile);
    setError(null);
    setIsUploading(true);

    try {
      const result = await uploadFile(selectedFile);
      setBatch(result);
      return result;
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Upload failed';
      setError(message);
      throw err;
    } finally {
      setIsUploading(false);
    }
  }, []);

  const reset = useCallback(() => {
    setFile(null);
    setError(null);
    setBatch(null);
  }, []);

  return {
    file,
    isUploading,
    error,
    batch,
    upload,
    reset,
    setError,
  };
}
