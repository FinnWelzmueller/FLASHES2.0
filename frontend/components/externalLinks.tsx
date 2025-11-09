import { degToHms, degToDms } from '@/lib/astro';

export function ExternalLinks({ name, raDeg, decDeg }: { name: string; raDeg: number; decDeg: number }) {
  const { hms } = degToHms(raDeg);
  const { dms } = degToDms(decDeg);

  const simbad = `https://simbad.u-strasbg.fr/simbad/sim-basic?Ident=${encodeURIComponent(name)}`;
  const ned = `https://ned.ipac.caltech.edu/byname?objname=${encodeURIComponent(name)}`;
  const heasarc = `https://heasarc.gsfc.nasa.gov/db-perl/W3Browse/w3query.pl?tablehead=name%3Dheasarc_master&Action=More&Coordinates=R.A.${hms}+Dec.${dms}`;

  return (
    <div className="text-sm text-neutral-500 dark:text-neutral-400 flex flex-wrap gap-3">
      <a className="underline hover:no-underline" href={simbad} target="_blank">SIMBAD</a>
      <a className="underline hover:no-underline" href={ned} target="_blank">NED</a>
      <a className="underline hover:no-underline" href={heasarc} target="_blank">HEASARC</a>
    </div>
  );
}