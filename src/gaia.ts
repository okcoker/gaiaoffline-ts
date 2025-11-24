import {
  GaiaDatabase,
  type GaiaRecord,
  type TrackingProgress,
} from "./database.ts";
import { type CLIConfig, DEFAULT_CONFIG } from "./config.ts";
import type { GaiaColumn, PhotometryOutput } from "./types.ts";

export type GaiaOptions = {
  /**
   * Path to the local database
   * @default ./gaiaoffline.db
   */
  databasePath?: CLIConfig["databasePath"];
  /**
   * The select columns to store in the database
   * @default ["source_id", "ra", "dec", "parallax", "pmra", "pmdec", "radial_velocity", "phot_g_mean_flux", "phot_bp_mean_flux", "phot_rp_mean_flux", "teff_gspphot", "logg_gspphot", "mh_gspphot"]
   */
  storedColumns?: CLIConfig["storedColumns"];
  /**
   * The zeropoints for the photometry
   * @default [25.6873668671, 25.3385422158, 24.7478955012]
   */
  zeropoints?: CLIConfig["zeropoints"];
  /**
   * The log level
   * @default "INFO"
   */
  logLevel?: CLIConfig["logLevel"];
  /**
   * The magnitude limit for the photometry
   * @default [-3, 20]
   */
  magnitudeLimit?: [number, number];
  /**
   * The limit for the query
   * @default 0
   */
  limit?: number;
  /**
   * The photometry output
   * @default "flux"
   */
  photometryOutput?: PhotometryOutput;
  /**
   * Whether to use 2MASS crossmatch
   * @default false
   */
  tmassCrossmatch?: boolean;
};

// 2MASS zeropoints (Vega system)
const tmassZeropoints = {
  j: 20.86650085,
  h: 20.6576004,
  k: 20.04360008,
};

/**
 * Gaia offline query interface
 * Port of the Python Gaia class
 */
export class Gaia {
  private db: GaiaDatabase;
  private options: Required<GaiaOptions>;

  constructor(options: GaiaOptions = {}) {
    this.options = {
      magnitudeLimit: options.magnitudeLimit || [-3, 20],
      limit: options.limit || 0,
      photometryOutput: options.photometryOutput || "flux",
      tmassCrossmatch: options.tmassCrossmatch || false,
      databasePath: options.databasePath || DEFAULT_CONFIG.databasePath,
      storedColumns: options.storedColumns || DEFAULT_CONFIG.storedColumns,
      zeropoints: options.zeropoints || DEFAULT_CONFIG.zeropoints,
      logLevel: options.logLevel || DEFAULT_CONFIG.logLevel,
    };
    this.db = new GaiaDatabase(this.options);

    // Check if 2MASS table exists if crossmatch is requested
    if (this.options.tmassCrossmatch && !this.db.hasTmassTable()) {
      throw new Error(
        "2MASS Crossmatch is not present in the database. Run populate:tmass first.",
      );
    }
  }

  /**
   * Perform a cone search around RA, Dec
   */
  coneSearch(ra: number, dec: number, radius: number): GaiaRecord[] {
    let results = this.db.coneSearch(
      ra,
      dec,
      radius,
      this.options.magnitudeLimit,
      this.options.tmassCrossmatch,
    );

    // Apply limit if specified
    if (this.options.limit > 0) {
      results = results.slice(0, this.options.limit);
    }

    // Convert photometry if needed
    return this.cleanDataFrame(results);
  }

  /**
   * Search for all targets within a brightness limit
   */
  brightnessLimitSearch(magnitudeLimit: [number, number]): GaiaRecord[] {
    // This would require a full table scan, so we'll use the cone search
    // with a very large radius as a proxy
    const results = this.db.coneSearch(
      0,
      0,
      180,
      magnitudeLimit,
      this.options.tmassCrossmatch,
    );

    if (this.options.limit > 0) {
      return this.cleanDataFrame(results.slice(0, this.options.limit));
    }

    return this.cleanDataFrame(results);
  }

  /**
   * Convert flux to magnitude or vice versa based on user preferences
   */
  private cleanDataFrame(records: GaiaRecord[]): GaiaRecord[] {
    if (this.options.photometryOutput === "magnitude") {
      return records.map((record) => {
        const cleaned = { ...record };

        // Handle Gaia photometry
        const bands = [
          { flux: "phot_g_mean_flux", mag: "phot_g_mean_mag", zp: 0 },
          { flux: "phot_bp_mean_flux", mag: "phot_bp_mean_mag", zp: 1 },
          { flux: "phot_rp_mean_flux", mag: "phot_rp_mean_mag", zp: 2 },
        ];

        for (const band of bands) {
          const flux = record[band.flux] as number;
          if (flux && flux > 0) {
            const zeropoint = this.options.zeropoints[band.zp];
            cleaned[band.mag] = zeropoint - 2.5 * Math.log10(flux);

            // Calculate magnitude error if flux error exists
            const fluxError = record[`${band.flux}_error`] as number;
            if (fluxError) {
              cleaned[`${band.mag}_error`] = (2.5 / Math.log(10)) *
                (fluxError / flux);
              delete cleaned[`${band.flux}_error`];
            }

            delete cleaned[band.flux];
          }
        }

        // 2MASS magnitudes are already in magnitude format, just ensure they're numeric
        if (this.options.tmassCrossmatch) {
          if (cleaned.j_m !== null && cleaned.j_m !== undefined) {
            cleaned.j_m = Number(cleaned.j_m);
          }
          if (cleaned.h_m !== null && cleaned.h_m !== undefined) {
            cleaned.h_m = Number(cleaned.h_m);
          }
          if (cleaned.k_m !== null && cleaned.k_m !== undefined) {
            cleaned.k_m = Number(cleaned.k_m);
          }
        }

        return cleaned;
      });
    }

    if (this.options.photometryOutput === "flux") {
      // Convert 2MASS magnitudes to flux if needed
      if (this.options.tmassCrossmatch) {
        return records.map((record) => {
          const cleaned = { ...record };

          if (cleaned.j_m !== null && cleaned.j_m !== undefined) {
            const jMag = Number(cleaned.j_m);
            cleaned.j_flux = 10 ** (-0.4 * (jMag - tmassZeropoints.j));
            delete cleaned.j_m;
          }

          if (cleaned.h_m !== null && cleaned.h_m !== undefined) {
            const hMag = Number(cleaned.h_m);
            cleaned.h_flux = 10 ** (-0.4 * (hMag - tmassZeropoints.h));
            delete cleaned.h_m;
          }

          if (cleaned.k_m !== null && cleaned.k_m !== undefined) {
            const kMag = Number(cleaned.k_m);
            cleaned.k_flux = 10 ** (-0.4 * (kMag - tmassZeropoints.k));
            delete cleaned.k_m;
          }

          return cleaned;
        });
      }
    }

    return records;
  }

  /**
   * Get database statistics
   */
  getStats(): {
    totalRecords: number;
    trackingProgress: { [key: string]: TrackingProgress | null };
  } {
    const totalRecords = this.db.getRecordCount();

    const trackingTables = [
      "file_tracking_gaiadr3",
      "file_tracking_tmass_xmatch",
      "file_tracking_tmass",
    ];

    const trackingProgress: { [key: string]: TrackingProgress | null } = {};

    for (const table of trackingTables) {
      try {
        trackingProgress[table] = this.db.getTrackingProgress(table);
      } catch {
        // Table doesn't exist yet
        trackingProgress[table] = null;
      }
    }

    return {
      totalRecords,
      trackingProgress,
    };
  }

  /**
   * Get column names from the database
   */
  getColumnNames(): GaiaColumn[] {
    return this.options.storedColumns;
  }

  /**
   * Benchmark query performance
   */
  benchmark(iterations = 100): number {
    const start = Date.now();

    for (let i = 0; i < iterations; i++) {
      this.coneSearch(45, 6, 0.2);
    }

    const duration = Date.now() - start;
    return duration / iterations;
  }

  /**
   * Close database connection
   */
  close(): void {
    this.db.close();
  }
}

interface WithGaiaCallback<T = void> {
  (gaia: Gaia): T | Promise<T>;
}

interface GaiaRunner {
  run<T>(withGaia: (gaia: Gaia) => T): T;
  run<T>(withGaia: (gaia: Gaia) => Promise<T>): Promise<T>;
}

/**
 * Convenience function to create a Gaia instance
 * @param options - The options for the Gaia instance
 * @returns The Gaia instance
 */
export function createGaia(
  options?: GaiaOptions,
): GaiaRunner {
  return {
    run<T>(withGaia: WithGaiaCallback<T>): T | Promise<T> {
      const g = new Gaia(options);
      try {
        const result = withGaia(g);
        if (result instanceof Promise) {
          return result.finally(() => g.close());
        }
        g.close();
        return result;
      } catch (error) {
        g.close();
        throw error;
      }
    },
  };
}
