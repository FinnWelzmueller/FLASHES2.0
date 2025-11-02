import { Tag } from '@/components/tag';
import { ExternalLinks } from '@/components/externalLinks';
import type { SourceData } from '@/src/app/sources/[_id]/page';
import { generateURL } from '@/lib/generateIframe';

export function SourceHeader({ data }: { data: SourceData }) {
  const iframe_src = generateURL({ data });
  return (
    <header className="flex flex-col gap-3 border-b border-neutral-200 dark:border-neutral-800 pb-6">
      <div className="flex items-start justify-between gap-4">
        <h1 className="text-3xl font-semibold tracking-tight">{data.integral_name}</h1>
        {iframe_src ? (
          <div className="w-[360px] max-w-full h-[120px] rounded-xl border border-neutral-200 dark:border-neutral-800 overflow-hidden shadow-sm">
            <iframe
              src={iframe_src}
              title={`Grafana preview for ${data.integral_name}`}
              loading="lazy"
              referrerPolicy="no-referrer"
              className="w-full h-full pointer-events-none select-none"
              width="360" height="120"
            />
          </div>
        ) : (
          <div className="text-sm text-neutral-400 italic">No Grafana data available</div>
        )}
      </div>

      <div className="flex flex-wrap items-center gap-2">
        {data.labels_constant?.map((label) => <Tag key={label} label={label} />)}
      </div>

      <ExternalLinks name={data.integral_name} raDeg={data.coord_ra} decDeg={data.coord_dec} />
    </header>
  );
}
