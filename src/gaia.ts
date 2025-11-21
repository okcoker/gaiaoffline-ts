import { GaiaDatabase, type GaiaRecord } from "./database.ts";
import type { CLIConfig } from "./config.ts";
import { GaiaColumn } from "./types.ts";

export type PhotometryOutput = "flux" | "magnitude";

export interface GaiaQueryOptions {
  magnitudeLimit?: [number, number];
  limit?: number;
  photometryOutput?: PhotometryOutput;
  tmassCrossmatch?: boolean;
}

/**
 * Gaia offline query interface
 * Port of the Python Gaia class
 */
export class Gaia {
  private db: GaiaDatabase;
  private config: CLIConfig;
  private options: Required<GaiaQueryOptions>;

  constructor(config: CLIConfig, options: GaiaQueryOptions = {}) {
    this.db = new GaiaDatabase(config);
    this.config = config;
    this.options = {
      magnitudeLimit: options.magnitudeLimit || [-3, 20],
      limit: options.limit || 0,
      photometryOutput: options.photometryOutput || "flux",
      tmassCrossmatch: options.tmassCrossmatch || false,
    };

    // Check if 2MASS table exists if crossmatch is requested
    if (this.options.tmassCrossmatch && !this.db.hasTmassTable()) {
      throw new Error(
        "2MASS Crossmatch is not present in the database. Run populate_tmass first.",
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
            const zeropoint = this.config.zeropoints[band.zp];
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
    } else if (this.options.photometryOutput === "flux") {
      // Convert 2MASS magnitudes to flux if needed
      if (this.options.tmassCrossmatch) {
        return records.map((record) => {
          const cleaned = { ...record };

          // 2MASS zeropoints (Vega system)
          const tmassZeropoints = {
            j: 20.86650085,
            h: 20.6576004,
            k: 20.04360008,
          };

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
    trackingProgress: { [key: string]: any };
  } {
    const totalRecords = this.db.getRecordCount();

    const trackingTables = [
      "file_tracking_gaiadr3",
      "file_tracking_tmass_xmatch",
      "file_tracking_tmass",
    ];

    const trackingProgress: { [key: string]: any } = {};

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
    return this.config.storedColumns;
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

/**
 * Convenience function to create a Gaia instance
 */
export function createGaia(
  config: CLIConfig,
  options?: GaiaQueryOptions,
) {
  return {
    run(withGaia: WithGaiaCallback) {
      const g = new Gaia(config, options);
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
