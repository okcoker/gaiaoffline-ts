import { parseArgs } from "@std/cli/parse-args";
import { GaiaColumn, isGaiaColumn, isLogLevel } from "./types.ts";

export interface CLIConfig {
  /**
   * Path to the local database
   * @default ./gaiaoffline.db
   */
  databasePath: string;
  /**
   * Number of max parallel downloads
   * @default 10
   */
  maxParallelDownloads: number;
  /**
   * Download directory
   * @default /tmp
   */
  downloadDir: string;
  /**
   * Whether to clean up downloaded files after processing
   * @default true
   */
  cleanUpDownloadedFiles: boolean;
  /**
   * The amount of rows to process at a time from the CSV file.
   * Lowering this number will reduce memory usage, but increase
   * the number of database inserts.
   * @default 100000
   */
  csvChunkSize: number;
  /**
   * The select columns to store in the database
   * @default ["source_id", "ra", "dec", "parallax", "pmra", "pmdec", "radial_velocity", "phot_g_mean_flux", "phot_bp_mean_flux", "phot_rp_mean_flux", "teff_gspphot", "logg_gspphot", "mh_gspphot"]
   */
  storedColumns: GaiaColumn[];
  /**
   * The zeropoints for the photometry
   * @default [25.6873668671, 25.3385422158, 24.7478955012]
   */
  zeropoints: number[];
  /**
   * The magnitude limit for the photometry
   * @default 16
   */
  magnitudeLimit: number;
  /**
   * The log level
   * @default "INFO"
   */
  logLevel: "DEBUG" | "INFO" | "WARN" | "ERROR";
}

export const DEFAULT_CONFIG: CLIConfig = {
  databasePath: "./gaiaoffline.db",
  maxParallelDownloads: 10,
  csvChunkSize: 100000,
  storedColumns: [
    "source_id",
    "ra",
    "dec",
    "parallax",
    "pmra",
    "pmdec",
    "radial_velocity",
    "phot_g_mean_flux",
    "phot_bp_mean_flux",
    "phot_rp_mean_flux",
    "teff_gspphot",
    "logg_gspphot",
    "mh_gspphot",
  ],
  downloadDir: getTempDir(),
  cleanUpDownloadedFiles: true,
  zeropoints: [25.6873668671, 25.3385422158, 24.7478955012],
  magnitudeLimit: 16,
  logLevel: "INFO",
};

export function parseConfig(args: string[]): CLIConfig {
  const parsed = parseArgs(args, {
    string: [
      "db-path",
      "log-level",
      "columns",
      "parallel",
      "mag-limit",
      "download-dir",
      "csv-chunks",
    ],
    boolean: [
      "clean",
    ],
    negatable: [
      "clean",
    ],
    default: {
      "db-path": DEFAULT_CONFIG.databasePath,
      parallel: DEFAULT_CONFIG.maxParallelDownloads,
      "download-dir": DEFAULT_CONFIG.downloadDir,
      "clean": DEFAULT_CONFIG.cleanUpDownloadedFiles,
      "mag-limit": DEFAULT_CONFIG.magnitudeLimit,
      "log-level": DEFAULT_CONFIG.logLevel,
      "csv-chunks": DEFAULT_CONFIG.csvChunkSize,
    },
    alias: {
      p: "parallel",
      m: "mag-limit",
      l: "log-level",
    },
  });

  const parallel = parseInt(`${parsed.parallel}`) ??
    DEFAULT_CONFIG.maxParallelDownloads;
  const magnitudeLimit = parseInt(`${parsed["mag-limit"]}`) ??
    DEFAULT_CONFIG.magnitudeLimit;
  const csvChunkSize = parseInt(`${parsed["csv-chunks"]}`) ||
    DEFAULT_CONFIG.csvChunkSize;
  const { valid, invalid } =
    (parsed.columns?.split(",") ?? DEFAULT_CONFIG.storedColumns).reduce(
      (acc: { valid: GaiaColumn[]; invalid: string[] }, column) => {
        if (isGaiaColumn(column)) {
          acc.valid.push(column);
        } else {
          acc.invalid.push(column);
        }
        return acc;
      },
      { valid: [], invalid: [] },
    );

  if (invalid.length > 0) {
    throw new Error(`Invalid columns: ${invalid.join(", ")}`);
  }

  const logLevel = parsed["log-level"].toUpperCase();
  if (!isLogLevel(logLevel)) {
    throw new Error(`Invalid log level: ${logLevel}`);
  }

  const config: CLIConfig = {
    databasePath: parsed["db-path"],
    maxParallelDownloads: clamp(parallel, 1, 50),
    downloadDir: parsed["download-dir"],
    cleanUpDownloadedFiles: parsed["clean"],
    magnitudeLimit,
    csvChunkSize,
    logLevel,
    storedColumns: valid,
    zeropoints: DEFAULT_CONFIG.zeropoints,
  };

  return config;
}

export function printUsage() {
  console.log(`
Usage: gaiaoffline [command] [options]

Commands:
  populate          Download and populate the Gaia database
  query             Run interactive queries (WIP)

Options:
  --clean           Clean up downloaded files after processing (default: true)
  --columns         Comma-separated list of columns to store (default: source_id,ra,dec,parallax,pmra,pmdec,radial_velocity,phot_g_mean_flux,phot_bp_mean_flux,phot_rp_mean_flux,teff_gspphot,logg_gspphot,mh_gspphot)
  --csv-chunks      The amount of rows to process at a time from the CSV file. (default: 100000)
  --db-path     Path to SQLite database (default: ./gaiaoffline.db)
  -l, --log-level   Log level: DEBUG, INFO, WARN, ERROR (default: INFO)
  --no-clean        Don't clean up downloaded files after processing
  -m, --mag-limit   Magnitude limit for filtering (default: 16)
  -p, --parallel    Number of parallel downloads (default: 10, max: 50)
Examples:
  # Populate with default settings
  gaiaoffline populate

  # Custom database location with 20 parallel downloads
  gaiaoffline populate --db-path /data/gaia.db --parallel 20

  # Download only specific columns
  gaiaoffline populate --columns source_id,ra,dec,phot_g_mean_flux
  `);
}

/**
 * Clamp a value between a minimum and maximum
 * @param value - The value to clamp
 * @param min - The minimum value
 * @param max - The maximum value
 * @returns The clamped value
 */
function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(value, max));
}

/**
 * Get the temporary directory
 * @returns The temporary directory
 */
function getTempDir(): string {
  return Deno.env.get("TMPDIR") || Deno.env.get("TMP") ||
    Deno.env.get("TEMP") || "/tmp";
}
