import { useState, useEffect, useCallback, useRef } from 'react';
import type { ProgressEvent } from '../types';
import { createProgressEventSource } from '../services/api';
import { authService } from '../services/auth';

export function useProgressTracking(batchId: string | null) {
  const [progress, setProgress] = useState<ProgressEvent | null>(null);
  const [isConnected, setIsConnected] = useState(false);
  const [completedBatchId, setCompletedBatchId] = useState<string | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);

  const closeConnection = useCallback(() => {
    if (eventSourceRef.current) {
      console.log('[SSE] Closing connection');
      eventSourceRef.current.close();
      eventSourceRef.current = null;
      setIsConnected(false);
    }
  }, []);

  const connect = useCallback(
    (id: string) => {
      // Close any existing connection first
      closeConnection();

      // Add token to query param for SSE auth
      const token = authService.getToken();
      const tokenParam = token ? `?token=${token}` : '';
      const eventSource = createProgressEventSource(id + tokenParam);
      eventSourceRef.current = eventSource;

      eventSource.onopen = () => {
        console.log('[SSE] Connection opened for batch:', id);
        setIsConnected(true);
      };

      eventSource.onmessage = (event) => {
        try {
          const parsed = JSON.parse(event.data);
          const progressData: ProgressEvent = parsed;

          console.log(
            '[SSE] Received:',
            progressData.status,
            progressData.percentage + '%',
            'for batch:',
            id,
          );
          setProgress(progressData);

          // Track which batch completed
          if (progressData.status === 'completed') {
            console.log('[SSE] Marking batch as completed:', id);
            setCompletedBatchId(id);
          }

          // IMMEDIATELY close when completed or failed
          if (
            progressData.status === 'completed' ||
            progressData.status === 'failed'
          ) {
            console.log('[SSE] Work done, closing connection NOW');
            eventSource.close();
            eventSourceRef.current = null;
            setIsConnected(false);
          }
        } catch (e) {
          console.error('[SSE] Parse error:', e, event.data);
        }
      };

      eventSource.onerror = (e) => {
        console.error('[SSE] Error:', e);
        eventSource.close();
        eventSourceRef.current = null;
        setIsConnected(false);
      };

      return eventSource;
    },
    [closeConnection],
  );

  useEffect(() => {
    console.log('[SSE Effect] batchId changed to:', batchId);

    if (!batchId) {
      closeConnection();
      // Clear all state when no batch is being tracked
      setProgress(null);
      setCompletedBatchId(null);
      return;
    }

    // CRITICAL: Reset ALL state when starting to track a NEW batch
    console.log('[SSE Effect] Resetting state for new batch:', batchId);
    setProgress(null);
    setCompletedBatchId(null);

    connect(batchId);

    // Cleanup on unmount or batchId change
    return () => {
      closeConnection();
    };
  }, [batchId, connect, closeConnection]);

  // isComplete is TRUE only if:
  // 1. The current batchId matches the completedBatchId (prevents stale completion)
  // 2. Progress status is "completed"
  const isComplete =
    batchId !== null &&
    completedBatchId === batchId &&
    progress?.status === 'completed';
  const isFailed = progress?.status === 'failed';

  console.log(
    '[SSE Render] batchId:',
    batchId,
    'completedBatchId:',
    completedBatchId,
    'isComplete:',
    isComplete,
  );

  return {
    progress,
    isConnected,
    isComplete,
    isFailed,
  };
}
