import type { CLIConfig } from "../config.ts";
import { createGaia } from "../gaia.ts";

/**
 * Get the statistics of the database
 * @param config - The configuration for the database
 * @returns void
 */
export function statsCommand(config: CLIConfig) {
  console.log("📊 Gaia Offline - Database Statistics\n");

  const instance = createGaia({
    ...config,
    magnitudeLimit: undefined,
  });

  instance.run((gaia) => {
    const stats = gaia.getStats();

    console.log(`Database: ${config.databasePath}`);
    console.log(`Total records: ${stats.totalRecords.toLocaleString()}\n`);

    console.log("Tracking Progress:");
    console.log("─".repeat(30));

    for (const [table, progress] of Object.entries(stats.trackingProgress)) {
      if (progress) {
        const percentage = progress.total > 0
          ? ((progress.completed / progress.total) * 100).toFixed(1)
          : "0";

        const tableName = table.replace("file_tracking_", "");
        console.log(`${tableName}:`);
        console.log(`  Progress:  ${percentage}%`);
        console.log(`  Completed: ${progress.completed}/${progress.total}`);
        console.log(`  Failed:    ${progress.failed}`);
        console.log(`  Pending:   ${progress.pending}`);
        console.log();
      }
    }
  });
}
