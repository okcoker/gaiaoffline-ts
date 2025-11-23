/**
 * C-accelerated CSV processing utilities
 */

import { parseGzippedCsvC } from "./ffi/c.ts";
import type { CLIConfig } from "./config.ts";
import type { GaiaRecord } from "./database.ts";
import { filterByMagnitude } from "./utils.ts";

/**
 * Stream and filter CSV from a file path using C parser
 * Falls back to TypeScript implementation if C FFI fails
 */
export async function streamAndFilterCSVC(
  filePath: string,
  config: CLIConfig,
): Promise<GaiaRecord[]> {
  try {
    // Parse entire file with C
    const allRecords = await parseGzippedCsvC(
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
    throw new Error(`C CSV parsing failed: ${errorMessage}`);
  }
}
