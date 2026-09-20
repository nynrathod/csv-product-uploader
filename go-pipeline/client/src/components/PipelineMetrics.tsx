import { useEffect, useState } from 'react';
import { API_BASE } from '../services/constants';

interface Metrics {
  generatedAt: string;
  import: {
    fileName: string;
    status: string;
    ingestRowsPerSec: number;
    catalogRowsPerSec: number;
  };
  latency: { samples: number; p50Ms: number; p95Ms: number; maxMs: number };
  stream: { eventsInFlight: number; catalogLag: number; projectionLag: number };
  zeroLoss?: boolean;
}

const fmt = (n: number) =>
  n.toLocaleString('en-US', { maximumFractionDigits: 0 });

export function PipelineMetrics() {
  const [m, setM] = useState<Metrics | null>(null);

  useEffect(() => {
    const load = async () => {
      try {
        setM(await (await fetch(`${API_BASE}/api/v1/metrics`)).json());
      } catch {
        /* retry on next tick */
      }
    };
    load();
    const t = setInterval(load, 3000);
    return () => clearInterval(t);
  }, []);

  if (!m) return null;

  const cards = [
    {
      label: 'Ingest',
      value: `${fmt(m.import.ingestRowsPerSec)} rows/s`,
      hint: 'CSV → Kafka',
    },
    {
      label: 'Catalog',
      value: `${fmt(m.import.catalogRowsPerSec)} rows/s`,
      hint: 'events → PostgreSQL',
    },
    {
      label: 'p50 latency',
      value: `${Math.round(m.latency.p50Ms)} ms`,
      hint: 'event → database',
    },
    {
      label: 'p95 latency',
      value: `${Math.round(m.latency.p95Ms)} ms`,
      hint: `${fmt(m.latency.samples)} samples`,
    },
    {
      label: 'In flight',
      value: fmt(m.stream.eventsInFlight),
      hint: 'events on the stream',
    },
    {
      label: 'Lag',
      value: fmt(m.stream.catalogLag),
      hint: 'catalog-writer group',
    },
    {
      label: 'Zero loss',
      value:
        m.zeroLoss === undefined ? '—' : m.zeroLoss ? 'verified' : 'in flight',
      hint: m.zeroLoss ? 'published = confirmed' : 'pending confirmation',
      good: m.zeroLoss,
    },
  ];

  return (
    <div className="mb-6 grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-7">
      {cards.map((c) => (
        <div
          key={c.label}
          className="rounded-lg border border-zinc-200 bg-white px-3 py-2.5 shadow-sm"
        >
          <div className="text-[11px] font-medium tracking-wide text-zinc-400 uppercase">
            {c.label}
          </div>
          <div
            className={`mt-0.5 text-sm font-semibold tabular-nums ${c.good === true ? 'text-green-600' : 'text-zinc-900'}`}
          >
            {c.value}
          </div>
          <div
            className="mt-0.5 truncate text-[11px] text-zinc-400"
            title={c.hint}
          >
            {c.hint}
          </div>
        </div>
      ))}
    </div>
  );
}
