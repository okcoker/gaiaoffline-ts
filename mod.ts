/**
 * Gaia Offline - TypeScript/Deno Port
 *
 * A high-performance library for querying Gaia DR3 star catalog data locally.
 *
 * @example
 * ```ts
 * import { createGaia, DEFAULT_CONFIG } from "./mod.ts";
 *
 * const gaia = createGaia(DEFAULT_CONFIG, {
 *   photometryOutput: "magnitude",
 *   magnitudeLimit: [-3, 20],
 * });
 *
 * const results = gaia.coneSearch(45, 6, 0.2);
 * console.log(`Found ${results.length} stars`);
 * gaia.close();
 * ```
 *
 * @module
 */

// Core classes
export { createGaia, Gaia } from "./src/gaia.ts";
export { GaiaDatabase } from "./src/database.ts";

// Configuration
export { DEFAULT_CONFIG } from "./src/config.ts";
export type { CLIConfig as Config } from "./src/config.ts";

// Types
export type {
  GaiaRecord,
  TmassRecord,
  TmassXmatchRecord,
} from "./src/database.ts";
export type { GaiaQueryOptions, PhotometryOutput } from "./src/gaia.ts";
