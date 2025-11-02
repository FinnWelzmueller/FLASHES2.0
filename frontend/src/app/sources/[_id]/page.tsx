import '@/src/app/globals.css';
import { SourceHeader } from '@/components/sourceHeader';
import { StatGrid } from '@/components/statGrid';
import { TelescopeSection } from '@/components/telescopeSection';

export type TelescopeRecord = {
  data_url: string;
  influx_key: string;
  last_timestamp: number; 
  last_flux?: number;
  last_error?: number;
};

export type SourceData = {
  _id: string;
  integral_name: string;
  coord_ra: number;   // deg
  coord_dec: number;  // deg
  labels_constant: string[];
  swift?: TelescopeRecord | null;
  maxi?: TelescopeRecord | null;
  fermi?: TelescopeRecord | null;
  hardness_ratio?: { influx_key: string; last_timestamp: number } | null;
  combined?: { influx_key: string; last_timestamp: number } | null;
};

export default async function SourceDetails({ params }: { params: Promise<{ _id: string }> }) {
  const _id = (await params)._id;
  const res = await fetch(`http://localhost:8000/sources/${encodeURIComponent(String(_id))}`, { next: { revalidate: 0 } });
  if (!res.ok) throw new Error('The FLASHES backend appears to be down. Please try again later.');
  const data: SourceData = await res.json();

  return (
    <main className="max-w-7xl mx-auto px-4 py-8">
      <SourceHeader data={data} />

      <section className="mt-6">
        <StatGrid data={data} />
      </section>

      <section className="mt-10">
        <TelescopeSection data={data} />
      </section>
    </main>
  );
}
