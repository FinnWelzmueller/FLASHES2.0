// src/components/source/bits/OpenActions.tsx
import type { SourceData } from '@/src/app/sources/[_id]/page';

export function OpenActions({ data }: { data: SourceData }) {
  return (
    <div className="flex gap-2">
      <a
        href={`http://localhost:8000/plots/${encodeURIComponent(data._id)}`}
        target="_blank"
        rel="noreferrer"
        className="px-3 py-1.5 rounded-xl bg-neutral-900 text-white dark:bg-neutral-100 dark:text-black hover:opacity-90"
      >
        Open in Grafana
      </a>
      {data.combined?.influx_key && (
        <a
          href={`/download/${encodeURIComponent(data.combined.influx_key)}?start=&end=`}
          className="px-3 py-1.5 rounded-xl border border-neutral-300 dark:border-neutral-700 hover:bg-neutral-50/50 dark:hover:bg-neutral-900/30"
        >
          Download CSV
        </a>
      )}
    </div>
  );
}
