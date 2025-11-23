import { CsvParseStream } from "@std/csv/parse-stream";
import {
  GaiaDatabase,
  type GaiaRecord,
  type TmassRecord,
  type TmassXmatchRecord,
} from "./database.ts";
import type { CLIConfig } from "./config.ts";
import { Logger, LogLevel } from "./types.ts";
import { parse as parsePSV } from "@std/csv";

/**
 * Stream a gzipped CSV from a ReadableStream or file path
 */
async function* streamGzippedCSV(
  source: string | ReadableStream<Uint8Array>,
  columnsToKeep: string[],
  chunkSize: number,
): AsyncGenerator<GaiaRecord[]> {
  const columnsToKeepSet = new Set(columnsToKeep);
  const stringColumns = new Set(["source_id", "solution_id", "designation"]);
  let fileHandle: Deno.FsFile | null = null;

  try {
    // Get the readable stream from either file or direct stream
    let readable: ReadableStream<Uint8Array>;
    if (typeof source === "string") {
      fileHandle = await Deno.open(source, { read: true });
      readable = fileHandle.readable;
    } else {
      readable = source;
    }

    const csvStream = readable
      .pipeThrough(
        new DecompressionStream("gzip") as unknown as ReadableWritablePair<
          Uint8Array,
          Uint8Array
        >,
      )
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(
        new CsvParseStream({
          comment: "#",
        }),
      );

    let chunk: GaiaRecord[] = [];
    let headers: string[] = [];
    let columnIndices: number[] = [];
    let isFirstRow = true;

    for await (const record of csvStream) {
      if (isFirstRow) {
        headers = Object.values(record);
        // Pre-compute which column indices to keep
        columnIndices = headers
          .map((col, i) => columnsToKeepSet.has(col) ? i : -1)
          .filter((i) => i !== -1);
        isFirstRow = false;
        continue;
      }

      const recordObj: Record<string, unknown> = {};
      const values = Object.values(record);

      // Only process columns we need (pre-filtered by index)
      for (const i of columnIndices) {
        const col = headers[i];
        const value = values[i];

        // Fast path: keep strings as-is
        if (stringColumns.has(col) || value === "") {
          recordObj[col] = value;
        } else if (typeof value === "string") {
          // Inline number conversion without function call overhead
          const lower = value.toLowerCase();
          if (lower === "null") {
            recordObj[col] = null;
          } else if (lower === "false") {
            recordObj[col] = false;
          } else if (lower === "true") {
            recordObj[col] = true;
          } else {
            const num = Number(value);
            recordObj[col] = isNaN(num) ? value : num;
          }
        } else {
          recordObj[col] = value;
        }
      }

      chunk.push(recordObj as GaiaRecord);

      if (chunk.length >= chunkSize) {
        yield chunk;
        chunk = [];
      }
    }

    // Yield remaining records
    if (chunk.length > 0) {
      yield chunk;
    }
  } catch (error) {
    if (fileHandle) {
      try {
        fileHandle.close();
      } catch {
        // Ignore if already closed
      }
    }
    throw error;
  }
}

/**
 * Stream and filter CSV from a file path or download stream
 */
export async function streamAndFilterCSV(
  source: string | ReadableStream<Uint8Array>,
  config: CLIConfig,
): Promise<GaiaRecord[]> {
  const allRecords: GaiaRecord[] = [];

  for await (
    const chunk of streamGzippedCSV(
      source,
      config.storedColumns,
      config.csvChunkSize,
    )
  ) {
    const filteredRecords = filterByMagnitude(
      chunk,
      config.magnitudeLimit,
      config.zeropoints[0],
    );
    allRecords.push(...filteredRecords);
  }

  return allRecords;
}

/**
 * @deprecated Use streamAndFilterCSV instead
 */
export async function streamAndFilterFromURL(
  stream: ReadableStream<Uint8Array>,
  config: CLIConfig,
): Promise<GaiaRecord[]> {
  return streamAndFilterCSV(stream, config);
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
 * Process a single CSV file: stream, parse in chunks, filter, and insert
 */
export async function processCSVFile(
  filePath: string,
  url: string,
  db: GaiaDatabase,
  config: CLIConfig,
  trackingTable: string,
): Promise<{ success: boolean; recordCount: number; error?: string }> {
  try {
    if (db.isFileProcessed(trackingTable, url)) {
      console.log(`Skipping already processed file: ${url}`);
      return { success: true, recordCount: 0 };
    }

    let totalInserted = 0;

    for await (
      const chunk of streamGzippedCSV(
        filePath,
        config.storedColumns,
        config.csvChunkSize,
      )
    ) {
      // Filter by magnitude
      const filteredRecords = filterByMagnitude(
        chunk,
        config.magnitudeLimit,
        config.zeropoints[0],
      );

      const insertedCount = db.insertGaiaRecords(filteredRecords);
      totalInserted += insertedCount;
    }

    db.markFileCompleted(trackingTable, url);

    return { success: true, recordCount: totalInserted };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);

    // If gzip is corrupt, delete the file so it can be re-downloaded
    if (errorMessage.includes("corrupt gzip stream")) {
      try {
        await Deno.remove(filePath);
      } catch {
        // empty
      }
    }

    db.markFileFailed(trackingTable, url);

    return {
      success: false,
      recordCount: 0,
      error: errorMessage,
    };
  } finally {
    try {
      if (config.cleanUpDownloadedFiles) {
        await Deno.remove(filePath);
      }
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
  if (ms < 1000) {
    return `${ms}ms`;
  }

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
 * Process a 2MASS crossmatch CSV file
 * Matches Gaia source_id with 2MASS source_id
 *
 * @param potentialRecords - Optional pre-parsed records (from FFI parsers)
 */
export async function processTmassXmatchFile(
  filePath: string,
  url: string,
  db: GaiaDatabase,
  config: CLIConfig,
  trackingTable: string,
  potentialRecords?: TmassXmatchRecord[],
): Promise<{ success: boolean; recordCount: number; error?: string }> {
  let file: Deno.FsFile | null = null;

  try {
    // Check if already processed
    if (db.isFileProcessed(trackingTable, url)) {
      console.log(`Skipping already processed file: ${url}`);
      return { success: true, recordCount: 0 };
    }

    // If records not provided, parse with TypeScript
    if (!potentialRecords) {
      file = await Deno.open(filePath, { read: true });

      const csvStream = file.readable
        .pipeThrough(
          new DecompressionStream("gzip") as unknown as ReadableWritablePair<
            Uint8Array,
            Uint8Array
          >,
        )
        .pipeThrough(new TextDecoderStream())
        .pipeThrough(
          new CsvParseStream({
            skipFirstRow: true,
          }),
        );

      // Collect all potential crossmatch records first
      potentialRecords = [];

      for await (const row of csvStream) {
        const sourceId = row.source_id;
        const tmassSourceId = row.original_ext_source_id;

        if (sourceId && tmassSourceId) {
          potentialRecords.push({
            gaiadr3_source_id: sourceId,
            tmass_source_id: tmassSourceId,
          });
        }
      }

      // File stream is done, close it
      try {
        file.close();
      } catch {
        // Already closed by stream
      }
      file = null;
    }

    // Filter records using a single SQL query with IN clause (much faster)
    // Process in batches to avoid SQL length limits
    const batchSize = 1000;
    const xmatchRecords: TmassXmatchRecord[] = [];

    for (let i = 0; i < potentialRecords.length; i += batchSize) {
      const batch = potentialRecords.slice(i, i + batchSize);
      const sourceIds = batch.map((r) => `'${r.gaiadr3_source_id}'`).join(",");

      const existingIds = db
        .prepare(
          `SELECT source_id FROM gaiadr3 WHERE source_id IN (${sourceIds})`,
        )
        .all() as { source_id: string }[];

      const existingIdSet = new Set(existingIds.map((r) => r.source_id));

      // Only keep records where Gaia source exists
      for (const record of batch) {
        if (existingIdSet.has(record.gaiadr3_source_id)) {
          xmatchRecords.push(record);
        }
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
    // Close file if still open
    if (file) {
      try {
        file.close();
      } catch {
        // Already closed
      }
    }

    // Clean up downloaded file
    try {
      if (config.cleanUpDownloadedFiles) {
        await Deno.remove(filePath);
      }
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
  let file: Deno.FsFile | null = null;

  try {
    // Check if already processed
    if (db.isFileProcessed(trackingTable, url)) {
      console.log(`Skipping already processed file: ${url}`);
      return { success: true, recordCount: 0 };
    }

    // Stream pipe-delimited 2MASS file line-by-line
    file = await Deno.open(filePath, { read: true });

    const csvStream = file.readable
      .pipeThrough(
        new DecompressionStream("gzip") as unknown as ReadableWritablePair<
          Uint8Array,
          Uint8Array
        >,
      )
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(
        new CsvParseStream({
          separator: "|", // Pipe-delimited
          skipFirstRow: false, // No header row
        }),
      );

    // Parse pipe-delimited format and collect potential records
    // Columns we need: 5=tmass_source_id, 6=j_m, 10=h_m, 14=k_m
    const potentialRecords: Array<{
      tmass_source_id: string;
      j_m: number | null;
      h_m: number | null;
      k_m: number | null;
    }> = [];

    for await (const cols of csvStream) {
      // cols is an array of column values
      const colArray = Object.values(cols);

      if (colArray.length < 15) continue;

      const tmassSourceId = colArray[5]?.trim();
      if (!tmassSourceId) continue;

      const jMag = colArray[6]?.trim();
      const hMag = colArray[10]?.trim();
      const kMag = colArray[14]?.trim();

      potentialRecords.push({
        tmass_source_id: tmassSourceId,
        j_m: jMag && jMag !== "null" ? parseFloat(jMag) : null,
        h_m: hMag && hMag !== "null" ? parseFloat(hMag) : null,
        k_m: kMag && kMag !== "null" ? parseFloat(kMag) : null,
      });
    }

    // File stream is done, close it
    try {
      file.close();
    } catch {
      // Already closed by stream
    }
    file = null;

    // Match with xmatch table using batched queries
    const batchSize = 1000;
    const tmassRecords: TmassRecord[] = [];

    for (let i = 0; i < potentialRecords.length; i += batchSize) {
      const batch = potentialRecords.slice(i, i + batchSize);
      const tmassSourceIds = batch.map((r) => `'${r.tmass_source_id}'`).join(
        ",",
      );

      const xmatchResults = db
        .prepare(
          `SELECT gaiadr3_source_id, tmass_source_id FROM tmass_xmatch WHERE tmass_source_id IN (${tmassSourceIds})`,
        )
        .all() as { gaiadr3_source_id: string; tmass_source_id: string }[];

      const xmatchMap = new Map(
        xmatchResults.map((r) => [r.tmass_source_id, r.gaiadr3_source_id]),
      );

      // Only keep records that have a crossmatch
      for (const record of batch) {
        const gaiaSourceId = xmatchMap.get(record.tmass_source_id);
        if (gaiaSourceId) {
          tmassRecords.push({
            gaiadr3_source_id: gaiaSourceId,
            tmass_source_id: record.tmass_source_id,
            j_m: record.j_m,
            h_m: record.h_m,
            k_m: record.k_m,
          });
        }
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
    // Close file if still open
    if (file) {
      try {
        file.close();
      } catch {
        // Already closed
      }
    }

    // Clean up downloaded file
    try {
      await Deno.remove(filePath);
    } catch {
      // Ignore cleanup errors
    }
  }
}

function getLogPrefix(level: LogLevel, tag: string): string {
  return [`[${level}]`, `[${new Date().toISOString()}]`, `[${tag}]`]
    .join(" ");
}

export const createLogger = (level: LogLevel, tag: string): Logger => {
  const levels: Record<LogLevel, number> = {
    DEBUG: 0,
    INFO: 1,
    WARN: 2,
    ERROR: 3,
  };

  return {
    error(...args: unknown[]) {
      if (levels[level] > levels["ERROR"]) {
        return;
      }
      const prefix = getLogPrefix("ERROR", tag);
      console.error(prefix, ...args);
    },
    warn(...args: unknown[]) {
      if (levels[level] > levels["WARN"]) {
        return;
      }
      const prefix = getLogPrefix("WARN", tag);
      console.warn(prefix, ...args);
    },
    info(...args: unknown[]) {
      if (levels[level] > levels["INFO"]) {
        return;
      }
      const prefix = getLogPrefix("INFO", tag);
      console.info(prefix, ...args);
    },
    debug(...args: unknown[]) {
      if (levels[level] > levels["DEBUG"]) {
        return;
      }
      const prefix = getLogPrefix("DEBUG", tag);
      console.debug(prefix, ...args);
    },
  };
};
