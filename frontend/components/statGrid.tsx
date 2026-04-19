import { mjdToIso } from '@/lib/astro';
import type { SourceData } from '@/src/app/sources/[_id]/page';
import { getConversionFactor, findChannel } from '@/lib/utils';
import { TelescopeRecord } from '@/types/telescopeRecord';

function LastPoint({ label, rec }: { label: string; rec?: TelescopeRecord | null }) {
  if (!rec?.last_flux || rec.last_error == null) return null;

  const conversionFactor = getConversionFactor(findChannel(rec));
  const val = (rec.last_flux * conversionFactor).toFixed(3);
  const err = (rec.last_error * conversionFactor).toFixed(3);
  const when = rec.last_timestamp ? mjdToIso(rec.last_timestamp) : '—';
  
  return (
    <div className="p-4 rounded-2xl border border-neutral-200 dark:border-neutral-800">
      <div className="text-xs font-bold uppercase tracking-wider text-white mb-1">{label}</div>
      <div className="text-lg font-medium">{val} ± {err} mCrab</div>
      <div className="text-xs text-white mt-1">Last point: {when}</div>
    </div>
  );
}

export function StatGrid({ data }: { data: SourceData }) {
  const combinedTs = data.combined?.last_timestamp ? mjdToIso(data.combined.last_timestamp) : '—';
  const hardnessTs = data.hardness_ratio?.last_timestamp ? mjdToIso(data.hardness_ratio.last_timestamp) : '—';
  const galB = typeof data.coord_gal_b === "number" ? data.coord_gal_b.toFixed(5) : "—";
  const galL = typeof data.coord_gal_l === "number" ? data.coord_gal_l.toFixed(5) : "—";

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
      <div className="p-4 rounded-2xl border border-neutral-200 dark:border-neutral-800">
        <div className="text-xs font-bold uppercase tracking-wider text-white mb-1">Coordinates</div>
        <div className="text-sm">
          Galactic Latitude: {galB}°<br />
          Galactic Longitude: {galL}°
        </div>
      </div>

      <div className="p-4 rounded-2xl border border-neutral-200 dark:border-neutral-800">
        <div className="text-xs font-bold uppercase tracking-wider text-white mb-1">Combined (last update)</div>
        <div className="text-sm">{combinedTs}</div>
      </div>

      <div className="p-4 rounded-2xl border border-neutral-200 dark:border-neutral-800">
        <div className="text-xs font-bold uppercase tracking-wider text-white mb-1">Hardness (last update)</div>
        <div className="text-sm">{hardnessTs}</div>
      </div>

      <LastPoint label="Swift last point" rec={data.swift} />
      <LastPoint label="MAXI last point" rec={data.maxi} />
      <LastPoint label="Fermi last point" rec={data.fermi} />
    </div>
  );
}
