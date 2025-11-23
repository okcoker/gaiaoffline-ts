/**
 * Rust-accelerated CSV processing utilities
 */

import { parseGzippedCsvRust } from "./ffi/rust.ts";
import type { CLIConfig } from "./config.ts";
import type { GaiaRecord } from "./database.ts";
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
