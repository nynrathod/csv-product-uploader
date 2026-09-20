import { useEffect, useState } from 'react';
import type { ImportJob } from '../types';
import { API_BASE } from '../services/constants';
import { getImport } from '../services/api';

// Live import progress over server-sent events, with a polling fallback
// when the event stream is unavailable (proxy buffering, network drop).
export function useImportProgress(jobId: string | null) {
  const [job, setJob] = useState<ImportJob | null>(null);
  const [isComplete, setIsComplete] = useState(false);
  const [usingPolling, setUsingPolling] = useState(false);

  useEffect(() => {
    if (!jobId) {
      setJob(null);
      setIsComplete(false);
      setUsingPolling(false);
      return;
    }
    setJob(null);
    setIsComplete(false);
    setUsingPolling(false);

    let closed = false;
    const es = new EventSource(`${API_BASE}/api/v1/imports/${jobId}/events`);

    const handle = (e: Event, done: boolean) => {
      try {
        const parsed: ImportJob = JSON.parse((e as MessageEvent).data);
        setJob(parsed);
        if (
          done ||
          parsed.status === 'completed' ||
          parsed.status === 'failed'
        ) {
          setIsComplete(true);
          es.close();
        }
      } catch {
        // Ignore malformed frames; the next event carries fresh state.
      }
    };

    es.addEventListener('progress', (e) => handle(e, false));
    es.addEventListener('done', (e) => handle(e, true));
    es.onerror = () => {
      if (closed) return;
      es.close();
      setUsingPolling(true);
    };

    return () => {
      closed = true;
      es.close();
    };
  }, [jobId]);

  useEffect(() => {
    if (!usingPolling || !jobId || isComplete) return;
    const t = setInterval(async () => {
      const j = await getImport(jobId).catch(() => null);
      if (!j) return;
      setJob(j);
      if (j.status === 'completed' || j.status === 'failed') {
        setIsComplete(true);
      }
    }, 1500);
    return () => clearInterval(t);
  }, [usingPolling, jobId, isComplete]);

  return { job, isComplete };
}
