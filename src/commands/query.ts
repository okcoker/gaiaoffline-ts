import type { CLIConfig } from "../config.ts";
import { createGaia } from "../gaia.ts";
import { parseArgs } from "@std/cli/parse-args";
import { PhotometryOutput } from "../types.ts";

/**
 * Query the database with the Gaia DR3 data
 * @param config - The configuration for the database
 * @param args - The arguments for the command
 * @returns void
 */
export function queryCommand(config: CLIConfig, args: string[]) {
  const parsed = parseArgs(args, {
    string: [
      "ra",
      "dec",
      "radius",
      "magnitude-limit",
      "limit",
      "photometry",
    ],
    boolean: [
      "xmatch",
    ],
  });

  if (!parsed.ra) {
    throw new Error("--ra is required");
  }

  const ra = parseFloat(parsed.ra);

  if (!parsed.dec) {
    throw new Error("--dec is required");
  }

  const dec = parseFloat(parsed.dec);

  if (!parsed.radius) {
    throw new Error("--radius is required");
  }

  const radius = parseFloat(parsed.radius);

  const instance = createGaia({
    ...config,
    limit: Number(parsed.limit) ?? 0,
    photometryOutput: getPhotometryOutput(parsed.photometry),
    magnitudeLimit: getMagnitudeLimit(parsed["magnitude-limit"]) ?? [-3, 20],
    tmassCrossmatch: parsed["xmatch"],
  });

  const results = instance.run((gaia) => {
    return gaia.coneSearch(ra, dec, radius);
  });

  console.log(results);
}

function getPhotometryOutput(
  photometry?: string,
): PhotometryOutput | undefined {
  if (!photometry) {
    return undefined;
  }

  if (photometry === "flux") {
    return "flux";
  }

  if (photometry === "magnitude") {
    return "magnitude";
  }

  throw new Error(
    `Invalid photometry output: ${photometry}. Must be "flux" or "magnitude".`,
  );
}

function getMagnitudeLimit(magLimit?: string): [number, number] | undefined {
  if (!magLimit) {
    return undefined;
  }

  const [minMag, maxMag] = magLimit.split(",").map(Number);

  if (isNaN(minMag) || isNaN(maxMag)) {
    throw new Error(
      `Invalid magnitude limit: ${magLimit}. Must be a comma-separated list of two numbers.`,
    );
  }

  return [minMag, maxMag];
}
