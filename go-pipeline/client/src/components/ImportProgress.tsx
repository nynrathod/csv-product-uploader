import type { ImportJob } from '../types';

export function ImportProgress({ job }: { job: ImportJob | null }) {
  const valid = job?.validRows ?? 0;
  const published = job?.publishedRows ?? 0;
  const processed = job?.processedRows ?? 0;

  const done = Math.max(processed, published);
  const percent =
    valid > 0 ? Math.min(100, Math.round((done / valid) * 100)) : 0;

  let phase = 'Preparing import';
  if (valid > 0 && published < valid) phase = 'Streaming & publishing events';
  else if (published >= valid && processed < published)
    phase = 'Catalog worker materializing records';
  else if (published > 0) phase = 'Complete';

  const counters = [
    { label: 'Total', value: job?.totalRows ?? 0 },
    { label: 'Valid', value: valid },
    { label: 'Invalid', value: job?.invalidRows ?? 0 },
    { label: 'Published', value: published },
    { label: 'Processed', value: processed },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between text-sm">
        <span className="font-medium text-zinc-700">{phase}</span>
        <span className="text-zinc-500 tabular-nums">
          {done.toLocaleString()} / {valid.toLocaleString()} · {percent}%
        </span>
      </div>

      <div className="h-2.5 w-full overflow-hidden rounded-full bg-zinc-100">
        <div
          className="h-full rounded-full bg-zinc-900 transition-all duration-500 ease-out"
          style={{ width: `${percent}%` }}
        />
      </div>

      <div className="grid grid-cols-3 gap-3 sm:grid-cols-5">
        {counters.map((c) => (
          <div
            key={c.label}
            className="rounded-lg border border-zinc-100 bg-white p-3 text-center"
          >
            <div className="text-[11px] font-medium tracking-wide text-zinc-400 uppercase">
              {c.label}
            </div>
            <div className="mt-1 text-sm font-semibold text-zinc-900 tabular-nums">
              {c.value.toLocaleString()}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
