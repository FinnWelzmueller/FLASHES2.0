import type { SourceData } from '@/src/app/sources/[_id]/page';
import type { TelescopeRecord } from '@/types/telescopeRecord';
import { mjdToIso } from '@/lib/astro';
import { getConversionFactor, findChannel } from '@/lib/utils';

function TelescopeCard({ name, rec }: { name: 'Swift' | 'MAXI' | 'Fermi'; rec?: TelescopeRecord | null}) {
  if (!rec) return null;
  const conversionFactor = getConversionFactor(findChannel(rec));

  const when = rec.last_timestamp ? mjdToIso(rec.last_timestamp) : '—';
  const flux = rec.last_flux != null && rec.last_error != null
    ? `${(rec.last_flux * conversionFactor).toFixed(3)} ± ${(rec.last_error * conversionFactor).toFixed(3)} mCrab`
    : '—';
  const apiBase = process.env.NEXT_PUBLIC_API_URL ?? "";
  const plotBase = process.env.NEXT_PUBLIC_PLOTS_URL ?? "";
  return (
    <div className="rounded-2xl border border-neutral-200 dark:border-neutral-800 p-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold">{name}</h3>
        <a
          href={rec.data_url}
          target="_blank"
          className="text-sm underline hover:no-underline text-neutral-600 dark:text-neutral-300"
        >
          Original data
        </a>
      </div>

      <div className="mt-3 text-sm">
        <div className="text-neutral-500">Last point</div>
        <div className="font-medium">{flux}</div>
        <div className="text-neutral-500 mt-0.5">{when}</div>
      </div>
      <div >
        <iframe 
          src={`${plotBase}/d-solo/telescope-card-${name.toLowerCase()}/telescope-card-${name.toLowerCase()}?orgId=1&from=now-7d&to=now&timezone=browser&var-influxkey=${encodeURIComponent(rec.influx_key)}&kiosk=tv&panelId=panel-1&__feature.dashboardSceneSolo=true`} 
          width="325" 
          height="200"
          className="pointer-events-none"></iframe>
      </div>
      {rec.influx_key && (
        <div className="mt-4 flex gap-2">
          <a
            href={`${apiBase}/download/${encodeURIComponent(rec.influx_key)}`}
            className="px-3 py-1.5 text-sm rounded-xl border border-neutral-300 dark:border-neutral-700 hover:bg-neutral-50/50 dark:hover:bg-neutral-900/30"
          >
            Download CSV
          </a>
        </div>
      )}
    </div>
  );
}

export function TelescopeSection({ data }: { data: SourceData }) {
  const apiBase = process.env.NEXT_PUBLIC_API_URL ?? "";
  return (
    <div className="space-y-6">

      <div className="flex flex-wrap gap-2">
        <a
        href={`${apiBase}/plots/${encodeURIComponent(data._id)}`}
        className="px-3 py-1.5 text-sm rounded-xl border border-neutral-300 dark:border-neutral-700 hover:bg-neutral-50/50 dark:hover:bg-neutral-900/30">
            <b>View Dashboard</b>
      </a>
        {data.combined?.influx_key && (
          <a
            href={`${apiBase}/download/${encodeURIComponent(data.combined.influx_key)}`}
            className="px-3 py-1.5 text-sm rounded-xl border border-neutral-300 dark:border-neutral-700 hover:bg-neutral-50/50 dark:hover:bg-neutral-900/30"
          >
            Download Combined CSV
          </a>
        )}
        {data.hardness_ratio?.influx_key && (
          <a
            href={`${apiBase}/download/${encodeURIComponent(data.hardness_ratio.influx_key)}`}
            className="px-3 py-1.5 text-sm rounded-xl border border-neutral-300 dark:border-neutral-700 hover:bg-neutral-50/50 dark:hover:bg-neutral-900/30"
          >
            Download Hardness CSV
          </a>
        )}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <TelescopeCard name="Swift" rec={data.swift} />
        <TelescopeCard name="MAXI" rec={data.maxi} />
        <TelescopeCard name="Fermi" rec={data.fermi} />
      </div>
    </div>
  );
}
