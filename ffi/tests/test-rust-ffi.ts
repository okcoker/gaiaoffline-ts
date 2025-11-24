// How to run:
// deno run --allow-read test-rust-ffi.ts

import { parseGzippedCsvRust } from "../../src/ffi/rust.ts";

const filePath = "./test.csv.gz";

const columnsToKeep = [
  "source_id",
  "ra",
  "dec",
  "parallax",
  "pmra",
  "pmdec",
  "radial_velocity",
  "phot_g_mean_flux",
  "phot_bp_mean_flux",
  "phot_rp_mean_flux",
  "teff_gspphot",
  "logg_gspphot",
  "mh_gspphot",
];

console.log("Reading:", filePath);
console.log("Using Rust FFI parser");

const start = performance.now();

const records = await parseGzippedCsvRust(filePath, columnsToKeep, 0);

const duration = (performance.now() - start) / 1000;

console.log(
  `\nParsed ${records.length.toLocaleString()} rows in ${duration.toFixed(2)}s`,
);
console.log(
  `Rate: ${Math.round(records.length / duration).toLocaleString()} rows/sec`,
);
