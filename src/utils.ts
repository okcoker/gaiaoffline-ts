import { parse } from "https://deno.land/std@0.218.0/csv/parse.ts";
import { readAll } from "https://deno.land/std@0.218.0/io/read_all.ts";
import {
  GaiaDatabase,
  type GaiaRecord,
  type TmassRecord,
  type TmassXmatchRecord,
} from "./database.ts";
import type { CLIConfig } from "./config.ts";
import { Logger, LogLevel } from "./types.ts";

/**
 * Parse a gzipped CSV file and return records
 */
export async function parseGzippedCSV(
  filePath: string,
  columns: string[],
  skipRows = 1000,
): Promise<GaiaRecord[]> {
  // Read and decompress the file
  const file = await Deno.open(filePath, { read: true });
  const compressed = await readAll(file);
  file.close();

  // Decompress using DecompressionStream
  const stream = new Response(compressed).body!
    .pipeThrough(new DecompressionStream("gzip"));

  console.log("utils.ts:30", stream);

  const decompressed = await new Response(stream).text();

  console.log("utils.ts:32", decompressed);

  // Split into lines and skip rows
  const lines = decompressed.split("\n");
  const dataLines = lines.slice(skipRows);

  // Parse CSV
  const parsed = parse(dataLines.join("\n"), {
    skipFirstRow: true,
    columns,
  });

  return parsed as GaiaRecord[];
}

/**
 * Filter records by magnitude limit
 */
export function filterByMagnitude(
  records: GaiaRecord[],
  magnitudeLimit: number,
  zeropoint: number,
): GaiaRecord[] {
  return records.filter((record) => {
    const flux = record.phot_g_mean_flux as number;
    if (!flux || flux <= 0) return false;

    const magnitude = zeropoint - 2.5 * Math.log10(flux);
    return magnitude < magnitudeLimit;
  });
}

/**
 * Process a single CSV file: download, parse, filter, and insert
 */
export async function processCSVFile(
  filePath: string,
  url: string,
  db: GaiaDatabase,
  config: CLIConfig,
  trackingTable: string,
): Promise<{ success: boolean; recordCount: number; error?: string }> {
  try {
    // Check if already processed
    if (db.isFileProcessed(trackingTable, url)) {
      console.log(`Skipping already processed file: ${url}`);
      return { success: true, recordCount: 0 };
    }

    // Parse CSV
    const records = await parseGzippedCSV(
      filePath,
      config.storedColumns,
      // Skip first 1000 rows like the Python version
      // Seems like the first 100 rows are comments
      1000,
    );

    console.log("utils.ts:86", records);

    // Filter by magnitude
    const filteredRecords = filterByMagnitude(
      records,
      config.magnitudeLimit,
      config.zeropoints[0],
    );

    console.log("utils.ts:95", filteredRecords);

    // Insert into database (sequential)
    const insertedCount = db.insertGaiaRecords(filteredRecords);

    console.log("utils.ts:100", insertedCount);

    // Mark as completed
    db.markFileCompleted(trackingTable, url);

    return { success: true, recordCount: insertedCount };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    db.markFileFailed(trackingTable, url);

    return {
      success: false,
      recordCount: 0,
      error: errorMessage,
    };
  } finally {
    // Clean up downloaded file
    try {
      // await Deno.remove(filePath);
    } catch {
      // Ignore cleanup errors
    }
  }
}

/**
 * Format bytes to human-readable string
 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";

  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`;
}

/**
 * Format duration to human-readable string
 */
export function formatDuration(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);

  if (hours > 0) {
    return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`;
  }
  return `${seconds}s`;
}

/**
 * Simple progress bar
 */
export function renderProgressBar(
  current: number,
  total: number,
  width = 40,
): string {
  const percentage = total > 0 ? (current / total) * 100 : 0;
  const filled = Math.floor((width * current) / total);
  const empty = width - filled;

  const bar = "█".repeat(filled) + "░".repeat(empty);
  return `[${bar}] ${percentage.toFixed(1)}% (${current}/${total})`;
}

/**
 * Process a 2MASS crossmatch CSV file
 * Matches Gaia source_id with 2MASS source_id
 */
export async function processTmassXmatchFile(
  filePath: string,
  url: string,
  db: GaiaDatabase,
  trackingTable: string,
): Promise<{ success: boolean; recordCount: number; error?: string }> {
  try {
    // Check if already processed
    if (db.isFileProcessed(trackingTable, url)) {
      console.log(`Skipping already processed file: ${url}`);
      return { success: true, recordCount: 0 };
    }

    // Parse CSV with specific columns for crossmatch
    const file = await Deno.open(filePath, { read: true });
    const compressed = await readAll(file);
    file.close();

    const stream = new Response(compressed).body!.pipeThrough(
      new DecompressionStream("gzip"),
    );
    const decompressed = await new Response(stream).text();

    // Parse CSV
    const parsed = parse(decompressed, {
      skipFirstRow: true,
      columns: ["source_id", "original_ext_source_id"],
    }) as Array<{ source_id: string; original_ext_source_id: string }>;

    // Filter: only keep records that exist in gaiadr3 table
    // Create temp records and check against existing Gaia data
    const xmatchRecords: TmassXmatchRecord[] = [];

    for (const row of parsed) {
      // Check if this Gaia source exists in our database
      const exists = db
        .getRecordCount(`gaiadr3 WHERE source_id = '${row.source_id}'`);
      if (exists > 0) {
        xmatchRecords.push({
          gaiadr3_source_id: row.source_id,
          tmass_source_id: row.original_ext_source_id,
        });
      }
    }

    // Insert crossmatch records
    const insertedCount = db.insertTmassXmatchRecords(xmatchRecords);

    // Mark as completed
    db.markFileCompleted(trackingTable, url);

    return { success: true, recordCount: insertedCount };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    db.markFileFailed(trackingTable, url);

    return {
      success: false,
      recordCount: 0,
      error: errorMessage,
    };
  } finally {
    // Clean up downloaded file
    try {
      await Deno.remove(filePath);
    } catch {
      // Ignore cleanup errors
    }
  }
}

/**
 * Process a 2MASS photometry file
 * Extracts J, H, K magnitudes and matches with crossmatch table
 */
export async function processTmassFile(
  filePath: string,
  url: string,
  db: GaiaDatabase,
  trackingTable: string,
): Promise<{ success: boolean; recordCount: number; error?: string }> {
  try {
    // Check if already processed
    if (db.isFileProcessed(trackingTable, url)) {
      console.log(`Skipping already processed file: ${url}`);
      return { success: true, recordCount: 0 };
    }

    // Read and decompress the 2MASS file (pipe-delimited format)
    const file = await Deno.open(filePath, { read: true });
    const compressed = await readAll(file);
    file.close();

    const stream = new Response(compressed).body!.pipeThrough(
      new DecompressionStream("gzip"),
    );
    const decompressed = await new Response(stream).text();

    // Parse pipe-delimited format
    // Columns we need: 5=tmass_source_id, 6=j_m, 10=h_m, 14=k_m
    const lines = decompressed.split("\n");
    const tmassRecords: TmassRecord[] = [];

    for (const line of lines) {
      if (!line.trim()) continue;

      const cols = line.split("|");
      if (cols.length < 15) continue;

      const tmassSourceId = cols[5]?.trim();
      if (!tmassSourceId) continue;

      // Get the Gaia source ID from xmatch table
      const xmatchResult = db
        .prepare(
          `SELECT gaiadr3_source_id FROM tmass_xmatch WHERE tmass_source_id = ?`,
        )
        .get(tmassSourceId) as { gaiadr3_source_id: string } | undefined;

      if (xmatchResult) {
        const jMag = cols[6]?.trim();
        const hMag = cols[10]?.trim();
        const kMag = cols[14]?.trim();

        tmassRecords.push({
          gaiadr3_source_id: xmatchResult.gaiadr3_source_id,
          tmass_source_id: tmassSourceId,
          j_m: jMag && jMag !== "null" ? parseFloat(jMag) : null,
          h_m: hMag && hMag !== "null" ? parseFloat(hMag) : null,
          k_m: kMag && kMag !== "null" ? parseFloat(kMag) : null,
        });
      }
    }

    // Insert 2MASS records
    const insertedCount = db.insertTmassRecords(tmassRecords);

    // Mark as completed
    db.markFileCompleted(trackingTable, url);

    return { success: true, recordCount: insertedCount };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    db.markFileFailed(trackingTable, url);

    return {
      success: false,
      recordCount: 0,
      error: errorMessage,
    };
  } finally {
    // Clean up downloaded file
    try {
      await Deno.remove(filePath);
    } catch {
      // Ignore cleanup errors
    }
  }
}

export const createLogger = (level: LogLevel, tag: string): Logger => {
  const levels: Record<LogLevel, number> = {
    DEBUG: 0,
    INFO: 1,
    WARN: 2,
    ERROR: 3,
  };
  const prefix = [`[${level}]`, `[${new Date().toISOString()}]`, `[${tag}]`]
    .join(" ");

  return {
    log(...args: unknown[]) {
      if (levels[level] > levels["DEBUG"]) {
        return;
      }
      console.log(prefix, ...args);
    },
    error(...args: unknown[]) {
      if (levels[level] > levels["ERROR"]) {
        return;
      }
      console.error(prefix, ...args);
    },
    warn(...args: unknown[]) {
      if (levels[level] > levels["WARN"]) {
        return;
      }
      console.warn(prefix, ...args);
    },
    info(...args: unknown[]) {
      if (levels[level] > levels["INFO"]) {
        return;
      }
      console.info(prefix, ...args);
    },
    debug(...args: unknown[]) {
      if (levels[level] > levels["DEBUG"]) {
        return;
      }
      console.debug(prefix, ...args);
    },
  };
};
