import { useEffect, useState } from 'react';
import { Button } from './ui/Button';
import { getImportReport } from '../services/api';
import type { ImportReport } from '../types';

interface ReportViewProps {
  jobId: string;
  onBack: () => void;
}

const stateStyles: Record<string, string> = {
  complete: 'bg-green-100 text-green-800',
  'in-flight': 'bg-blue-100 text-blue-800',
  discrepancy: 'bg-amber-100 text-amber-800',
  failed: 'bg-red-100 text-red-800',
};

export function ReportView({ jobId, onBack }: ReportViewProps) {
  const [report, setReport] = useState<ImportReport | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    getImportReport(jobId)
      .then((r) => {
        if (!cancelled) setReport(r);
      })
      .catch((e) => {
        if (!cancelled)
          setError(e instanceof Error ? e.message : 'Failed to load report');
      });
    return () => {
      cancelled = true;
    };
  }, [jobId]);

  if (error) {
    return (
      <div className="mx-auto max-w-xl pt-10 text-center">
        <p className="text-sm text-red-600">{error}</p>
        <div className="mt-4">
          <Button variant="outline" onClick={onBack}>
            Back to imports
          </Button>
        </div>
      </div>
    );
  }

  if (!report) {
    return (
      <div className="mx-auto max-w-2xl space-y-3 pt-10">
        {[0, 1, 2].map((i) => (
          <div
            key={i}
            className="h-28 animate-pulse rounded-lg border border-zinc-100 bg-zinc-50"
          />
        ))}
      </div>
    );
  }

  const cards = [
    {
      title: 'File rows',
      items: [
        ['Total', report.rows.total],
        ['Valid', report.rows.valid],
        ['Invalid', report.rows.invalid],
      ],
    },
    {
      title: 'Event stream',
      items: [
        ['Published', report.events.published],
        ['Processed', report.events.processed],
        ['Retried', report.events.retried],
        ['Dead-lettered', report.events.dead],
      ],
    },
    {
      title: 'Reconciliation',
      items: [
        ['Published', report.reconciliation.published],
        ['Confirmed', report.reconciliation.confirmed],
        ['Unconfirmed', report.reconciliation.unconfirmed],
      ],
    },
  ];

  return (
    <div className="animate-in fade-in mx-auto w-full max-w-2xl pt-6 duration-300">
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div className="space-y-1">
            <div className="flex items-center gap-2 text-sm text-zinc-500">
              <span
                className="cursor-pointer transition-colors hover:text-zinc-900"
                onClick={onBack}
              >
                Imports
              </span>
              <span className="text-zinc-300">/</span>
              <span className="font-medium text-zinc-900">
                {report.fileName}
              </span>
            </div>
            <h2 className="text-2xl font-bold tracking-tight text-zinc-900">
              Import report
            </h2>
          </div>
          <span
            className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold ${stateStyles[report.reconciliation.state] ?? 'bg-zinc-100 text-zinc-700'}`}
          >
            {report.reconciliation.state}
          </span>
        </div>

        {cards.map((card) => (
          <div
            key={card.title}
            className="rounded-lg border border-zinc-200 bg-white p-5 shadow-sm"
          >
            <h3 className="text-sm font-semibold text-zinc-900">
              {card.title}
            </h3>
            <dl className="mt-3 grid grid-cols-2 gap-4 sm:grid-cols-4">
              {card.items.map(([label, value]) => (
                <div key={String(label)}>
                  <dt className="text-[11px] font-medium tracking-wide text-zinc-400 uppercase">
                    {label}
                  </dt>
                  <dd className="mt-0.5 text-sm font-semibold text-zinc-900 tabular-nums">
                    {(value as number).toLocaleString()}
                  </dd>
                </div>
              ))}
            </dl>
          </div>
        ))}

        <p className="text-xs text-zinc-400">
          Confirmed counts events the catalog acknowledged as processed or
          dead-lettered; unconfirmed events are still in flight on the stream.
        </p>

        <div>
          <Button variant="outline" onClick={onBack}>
            ← Back to imports
          </Button>
        </div>
      </div>
    </div>
  );
}
