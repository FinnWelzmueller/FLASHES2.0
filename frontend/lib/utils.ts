import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"
import { TelescopeRecord } from "@/components/sourceTable-columns";
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function getConversionFactor(channel: string): number {
  /**
   * This function returns a conversion factor to convert counts into mCrab.
   * It is used as: mCrab = factor * counts/cm2/s
   */
  const conversionFactors: Record<string, number> = {
    "2-4": 1.0/2.1 * 1000, // MAXI
    "4-10": 1.0/1.2 * 1000, // MAXI
    "10-20": 1.0/0.4 * 1000, // MAXI
    "2-20": 1.0/3.2 * 1000, // MAXI
    "15-50": 1.0/0.22 * 1000, // Swift-BAT
    "12-50": 1.0/4.5 * 1000// Fermi-GBM
  };

  const factor = conversionFactors[channel];

  if (factor === undefined) {
    throw new Error(`No conversion factor defined for channel '${channel}'.`);
  }

  return factor;
}

export function findChannel(telescope:  TelescopeRecord): string {
  /**
   * This function returns the main energy channel string based on the telescope influx key. Mostly used to get the conversion factor.
   */
  if (telescope.influx_key.includes("swift")) {
    return "15-50";
  } 
  else if (telescope.influx_key.includes("maxi")) {
    return "2-20";
  }
  else if (telescope.influx_key.includes("fermi")) {
    return "12-50";
  }
  else {
    throw new Error(`Unknown telescope influx key: ${telescope.influx_key}`);
  }
}