import { CsvParseStream } from "@std/csv/parse-stream";
import {
  GaiaDatabase,
  type GaiaRecord,
  type TmassRecord,
  type TmassXmatchRecord,
} from "./database.ts";
import type { CLIConfig } from "./config.ts";
import { Logger, LogLevel } from "./types.ts";

/**
 * Stream a gzipped CSV from a ReadableStream or file path
 */
async function* streamGzippedCSV(
  source: string | ReadableStream<Uint8Array>,
  columnsToKeep: string[],
  chunkSize: number,
): AsyncGenerator<GaiaRecord[]> {
  const columnsToKeepSet = new Set(columnsToKeep);
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
      .pipeThrough(new DecompressionStream("gzip"))
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(
        new CsvParseStream({
          comment: "#",
        }),
      );

    let chunk: GaiaRecord[] = [];
    let headers: string[] = [];
    let isFirstRow = true;

    for await (const record of csvStream) {
      if (isFirstRow) {
        headers = Object.values(record);
        isFirstRow = false;
        continue;
      }

      const recordObj: Record<string, unknown> = {};
      const values = Object.values(record);
      for (let i = 0; i < headers.length; i++) {
        const col = headers[i];
        if (columnsToKeepSet.has(col)) {
          const value = values[i];
          recordObj[col] = parseGaiaRecord(col, value);
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

    // Parse CSV using streaming parser
    const file = await Deno.open(filePath, { read: true });

    const csvStream = file.readable
      .pipeThrough(new DecompressionStream("gzip"))
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(
        new CsvParseStream({
          skipFirstRow: true,
        }),
      );

    // Filter: only keep records that exist in gaiadr3 table
    const xmatchRecords: TmassXmatchRecord[] = [];

    for await (const row of csvStream) {
      const sourceId = row.source_id;
      const tmassSourceId = row.original_ext_source_id;

      if (sourceId && tmassSourceId) {
        // Check if this Gaia source exists in our database
        const exists = db
          .getRecordCount(`gaiadr3 WHERE source_id = '${sourceId}'`);
        if (exists > 0) {
          xmatchRecords.push({
            gaiadr3_source_id: sourceId,
            tmass_source_id: tmassSourceId,
          });
        }
      }
    }

    file.close();

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

    // Stream pipe-delimited 2MASS file line-by-line
    const file = await Deno.open(filePath, { read: true });

    const csvStream = file.readable
      .pipeThrough(new DecompressionStream("gzip"))
      .pipeThrough(new TextDecoderStream())
      .pipeThrough(
        new CsvParseStream({
          separator: "|", // Pipe-delimited
          skipFirstRow: false, // No header row
        }),
      );

    // Parse pipe-delimited format
    // Columns we need: 5=tmass_source_id, 6=j_m, 10=h_m, 14=k_m
    const tmassRecords: TmassRecord[] = [];

    for await (const cols of csvStream) {
      // cols is an array of column values
      const colArray = Object.values(cols);

      if (colArray.length < 15) continue;

      const tmassSourceId = colArray[5]?.trim();
      if (!tmassSourceId) continue;

      // Get the Gaia source ID from xmatch table
      const xmatchResult = db
        .prepare(
          `SELECT gaiadr3_source_id FROM tmass_xmatch WHERE tmass_source_id = ?`,
        )
        .get(tmassSourceId) as { gaiadr3_source_id: string } | undefined;

      if (xmatchResult) {
        const jMag = colArray[6]?.trim();
        const hMag = colArray[10]?.trim();
        const kMag = colArray[14]?.trim();

        tmassRecords.push({
          gaiadr3_source_id: xmatchResult.gaiadr3_source_id,
          tmass_source_id: tmassSourceId,
          j_m: jMag && jMag !== "null" ? parseFloat(jMag) : null,
          h_m: hMag && hMag !== "null" ? parseFloat(hMag) : null,
          k_m: kMag && kMag !== "null" ? parseFloat(kMag) : null,
        });
      }
    }

    file.close();

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

function parseGaiaRecord(column: string, value: unknown): unknown {
  if (
    column === "source_id" ||
    column === "solution_id" ||
    column === "designation" ||
    value === ""
  ) {
    return value;
  }

  if (typeof value === "string") {
    if (value.toLowerCase() === "null") {
      return null;
    }
    if (value.toLowerCase() === "false") {
      return false;
    }
    if (value.toLowerCase() === "true") {
      return true;
    }
  }

  if (column === "source_id" || value === "" || value === null) {
    return value;
  }
  const num = Number(value);
  return isNaN(num) ? value : num;
}
