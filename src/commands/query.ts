import type { CLIConfig } from "../config.ts";
import { createGaia } from "../gaia.ts";

/**
 * Query the database with the Gaia DR3 data
 * @param config - The configuration for the database
 * @returns void
 */
export function queryCommand(config: CLIConfig) {
  console.log("🔍 Gaia Offline - Query Interface\n");

  const instance = createGaia(config, {
    photometryOutput: "magnitude",
    magnitudeLimit: [-3, 20],
  });

  instance.run((gaia) => {
    // Get stats
    const stats = gaia.getStats();
    console.log(`Database: ${config.databasePath}`);
    console.log(`Total records: ${stats.totalRecords.toLocaleString()}\n`);

    // Check if tracking info exists
    if (stats.trackingProgress.file_tracking_gaiadr3) {
      const progress = stats.trackingProgress.file_tracking_gaiadr3;
      const percentage = progress.total > 0
        ? ((progress.completed / progress.total) * 100).toFixed(1)
        : "0";
      console.log(`Database population: ${percentage}% complete`);
      console.log(
        `  Completed: ${progress.completed}, Failed: ${progress.failed}, Pending: ${progress.pending}\n`,
      );
    }

    // Example query
    console.log("📍 Example: Cone search around RA=45°, Dec=6°, radius=0.2°");
    console.log("   (This searches for stars in the Hyades cluster region)\n");

    const start = Date.now();
    const results = gaia.coneSearch(45, 6, 0.2);
    const duration = Date.now() - start;

    console.log(`Found ${results.length} stars in ${duration}ms`);

    if (results.length > 0) {
      console.log("\nFirst 5 results:");
      console.log(
        "─".repeat(80),
      );

      for (let i = 0; i < Math.min(5, results.length); i++) {
        const star = results[i];
        console.log(`Star ${i + 1}:`);
        console.log(`  Source ID: ${star.source_id}`);
        console.log(`  RA:        ${star.ra.toFixed(6)}°`);
        console.log(`  Dec:       ${star.dec.toFixed(6)}°`);

        if (star.phot_g_mean_mag) {
          console.log(
            `  G mag:     ${Number(star.phot_g_mean_mag).toFixed(3)}`,
          );
        }

        if (star.parallax) {
          console.log(`  Parallax:  ${Number(star.parallax).toFixed(3)} mas`);
        }

        console.log();
      }
    }

    // Benchmark
    console.log("🏃 Running benchmark (100 queries)...");
    const avgTime = gaia.benchmark(100);
    console.log(`Average query time: ${avgTime.toFixed(2)}ms\n`);

    console.log("💡 To run custom queries, you can use the Gaia class:");
    console.log(`
import { createGaia } from "./gaia.ts";
import { DEFAULT_CONFIG } from "./config.ts";

const gaia = createGaia("${config.databasePath}", DEFAULT_CONFIG);
const results = gaia.coneSearch(ra, dec, radius);
console.log(results);
gaia.close();
    `);
  });
}
