import { Tag } from '@/components/tag';
import { ExternalLinks } from '@/components/externalLinks';
import type { SourceData } from '@/src/app/sources/[_id]/page';

export function SourceHeader({ data }: { data: SourceData }) {
  return (
    <header className="flex flex-col gap-3 border-b border-neutral-200 dark:border-neutral-800 pb-6">
      <div className="flex items-start justify-between gap-4">
        <h1 className="text-3xl font-semibold tracking-tight">{data.integral_name}</h1>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        {data.labels_constant?.map((label) => <Tag key={label} label={label} />)}
      </div>

      <ExternalLinks name={data.integral_name} raDeg={data.coord_ra} decDeg={data.coord_dec} />
    </header>
  );
}
