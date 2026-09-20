import { useState, useCallback, useEffect, useRef } from 'react';
import { authService } from '../services/auth';
import type { UploadBatch } from '../types';
import { fetchBatches } from '../services/api';

export function useBatchesData() {
  const [batches, setBatches] = useState<UploadBatch[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const isAuthenticated = Boolean(authService.getToken());

  const load = useCallback(async () => {
    if (!isAuthenticated) {
      console.log('[useBatchesData] Not authenticated, skipping fetch');
      setBatches([]);
      return;
    }

    console.log('[useBatchesData] Loading batches...');
    setLoading(true);
    setError(null);

    try {
      const response = await fetchBatches();
      console.log('[useBatchesData] Fetched batches:', response.data.length);
      setBatches(response.data);
    } catch (err) {
      const message =
        err instanceof Error ? err.message : 'Failed to load batches';
      console.error('[useBatchesData] Error:', message);
      setError(message);
    } finally {
      setLoading(false);
    }
  }, [isAuthenticated]);

  // Load on mount
  useEffect(() => {
    if (isAuthenticated) {
      load();
    }
  }, [isAuthenticated, load]);

  // Poll every 5 seconds if there are processing batches
  useEffect(() => {
    const hasProcessing = batches.some(
      (b) => b.status === 'processing' || b.status === 'pending',
    );

    if (hasProcessing && isAuthenticated) {
      // Start polling
      intervalRef.current = setInterval(() => {
        console.log('[useBatchesData] Polling for updates...');
        load();
      }, 5000);
    } else {
      // Stop polling
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    }

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [batches, isAuthenticated, load]);

  const refresh = useCallback(() => {
    load();
  }, [load]);

  return {
    batches,
    loading,
    error,
    refresh,
  };
}
