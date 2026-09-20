import { useCallback, useEffect, useState } from 'react';
import type { ImportJob } from '../types';
import { listImports } from '../services/api';

export function useImportsData() {
  const [imports, setImports] = useState<ImportJob[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      setError(null);
      setImports(await listImports());
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load imports');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  // While any import is in flight, keep the list current.
  const anyActive = imports.some(
    (j) => j.status === 'pending' || j.status === 'processing',
  );
  useEffect(() => {
    if (!anyActive) return;
    const t = setInterval(refresh, 2000);
    return () => clearInterval(t);
  }, [anyActive, refresh]);

  return { imports, loading, error, refresh };
}
