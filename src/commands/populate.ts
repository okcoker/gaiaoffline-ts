import type { CLIConfig } from "../config.ts";
import { PopulateCoordinator } from "../coordinator.ts";
import { GaiaDatabase } from "../database.ts";

type PopulateType = "all" | "gaia" | "tmass-xmatch" | "tmass";

/**
 * Populate the database with the Gaia DR3 data
 * @param config - The configuration for the database
 * @param args - The arguments for the command
 * @returns void
 */
export async function populateCommand(
  config: CLIConfig,
  type: PopulateType,
  args: string[],
): Promise<void> {
  console.log("🚀 Gaia Offline - Database Population\n");
  console.log(`Configuration:`);
  console.log(`  Database path:       ${config.databasePath}`);
  console.log(`  Parallel downloads:  ${config.maxParallelDownloads}`);
  console.log(`  Magnitude limit:     ${config.magnitudeLimit}`);
  console.log(
    `  Stored columns:      ${config.storedColumns.length} columns (${
      config.storedColumns.join(", ")
    })`,
  );
  console.log();

  // Parse additional populate options
  const fileLimit = args.includes("--file-limit")
    ? parseInt(args[args.indexOf("--file-limit") + 1])
    : undefined;

  if (fileLimit) {
    console.log(`⚠️  File limit: ${fileLimit} files (testing mode)\n`);
  }

  const db = new GaiaDatabase(config);
  const coordinator = new PopulateCoordinator(db, config);
  const cleanup = async () => {
    await coordinator.cleanup();
    db.close();
  };

  try {
    if (type === "all") {
      await coordinator.populateGaiaDR3(fileLimit);
      await coordinator.populateTmassXmatch(fileLimit);
      await coordinator.populateTmass(fileLimit);
    } else if (type === "gaia") {
      await coordinator.populateGaiaDR3(fileLimit);
    } else if (type === "tmass-xmatch") {
      await coordinator.populateTmassXmatch(fileLimit);
    } else if (type === "tmass") {
      await coordinator.populateTmass(fileLimit);
    }
    await cleanup();
  } catch (error) {
    await cleanup();
    throw error;
  }
}
