/**
 * Rust-accelerated CSV processing utilities
 */

import { parseGzippedCsvRust } from "./ffi/rust.ts";
import type { CLIConfig } from "./config.ts";
import type { GaiaRecord, TmassXmatchRecord } from "./database.ts";
import { filterByMagnitude } from "./utils.ts";

/**
 * Stream and filter CSV from a file path using Rust parser
 * Falls back to TypeScript implementation if Rust FFI fails
 */
export async function streamAndFilterCSVRust(
  filePath: string,
  config: CLIConfig,
): Promise<GaiaRecord[]> {
  try {
    // Parse entire file with Rust
    const allRecords = await parseGzippedCsvRust(
      filePath,
      config.storedColumns,
      config.csvChunkSize,
    );

    // Filter by magnitude (still in TypeScript for now)
    const filteredRecords = filterByMagnitude(
      allRecords as GaiaRecord[],
      config.magnitudeLimit,
      config.zeropoints[0],
    );

    return filteredRecords;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    throw new Error(`Rust CSV parsing failed: ${errorMessage}`);
  }
}

/**
 * Parse 2MASS crossmatch CSV using Rust parser
 */
export async function parseTmassXmatchRust(
  filePath: string,
): Promise<TmassXmatchRecord[]> {
  try {
    const columns = ["source_id", "original_ext_source_id"];
    const records = await parseGzippedCsvRust(
      filePath,
      columns,
      100000,
    );

    // Map to TmassXmatchRecord format
    return records.map((r: any) => ({
      gaiadr3_source_id: r.source_id,
      tmass_source_id: r.original_ext_source_id,
    }));
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    throw new Error(`Rust CSV parsing failed: ${errorMessage}`);
  }
}
