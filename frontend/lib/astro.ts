export function mjdToDate(mjd: number): Date {
  const ms = (mjd - 40587) * 86400 * 1000;
  return new Date(ms);
}
export function mjdToIso(mjd: number | string): string {
  try {
    if (typeof mjd === 'string') {
      mjd = parseFloat(mjd);
    }
    return mjdToDate(mjd).toISOString().replace('.000Z', 'Z');
  } catch {
    return '—';
  }
}

export function degToHms(raDeg: number) {
  // RA in Stunden
  const totalH = (raDeg / 360) * 24;
  const h = Math.floor(totalH);
  const m = Math.floor((totalH - h) * 60);
  const s = ((totalH - h) * 60 - m) * 60;
  const hms = `${String(h).padStart(2, '0')}h ${String(m).padStart(2, '0')}m ${s.toFixed(2)}s`;
  return { h, m, s, hms };
}

export function degToDms(decDeg: number) {
  const sign = decDeg >= 0 ? '+' : '-';
  const abs = Math.abs(decDeg);
  const d = Math.floor(abs);
  const m = Math.floor((abs - d) * 60);
  const s = ((abs - d) * 60 - m) * 60;
  const dms = `${sign}${String(d).padStart(2, '0')}° ${String(m).padStart(2, '0')}' ${s.toFixed(1)}"`;
  return { d: sign === '-' ? -d : d, m, s, dms };
}
