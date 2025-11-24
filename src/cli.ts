#!/usr/bin/env -S deno run --allow-net --allow-read --allow-write --allow-env

import { parseConfig, printUsage } from "./config.ts";
import { populateCommand } from "./commands/populate.ts";
import { queryCommand } from "./commands/query.ts";
import { statsCommand } from "./commands/stats.ts";

async function main(): Promise<void> {
  const args = Deno.args;

  if (args.length === 0 || args.includes("--help") || args.includes("-h")) {
    printUsage();
    Deno.exit(0);
  }

  const command = args[0];
  const config = parseConfig(args.slice(1));

  try {
    switch (command) {
      case "populate":
        await populateCommand(config, "all", args.slice(1));
        break;

      case "populate:gaia":
        await populateCommand(config, "gaia", args.slice(1));
        break;

      case "populate:tmass-xmatch":
        await populateCommand(config, "tmass-xmatch", args.slice(1));
        break;

      case "populate:tmass":
        await populateCommand(config, "tmass", args.slice(1));
        break;

      case "query":
        queryCommand(config, args.slice(1));
        break;

      case "stats":
        statsCommand(config);
        break;

      default:
        console.error(`Unknown command: ${command}\n`);
        printUsage();
        Deno.exit(1);
    }
  } catch (error) {
    console.error("❌ Failed to run command:\n", error);
    Deno.exit(1);
  }
}

if (import.meta.main) {
  main();
}
