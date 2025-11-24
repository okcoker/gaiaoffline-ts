# Gaia Offline - TS

A Deno port of the [`gaiaoffline`](https://github.com/christinahedges/gaiaoffline) Python library for downloading and querying Gaia DR3 star catalog data locally.

## Why

I created this port for usage of the library within a TS project, without having to rely on setting up a Python venv. I also wanted to experiment a bit with Deno's FFI interop.

## Features

1. **⚡ Parallel Downloads**: Downloads simultaneously (configurable via `--parallel` option)
1. **🔄 Streamed Downloads**: Automatically downloads and streams contents into local DB without writing (using `--stream`)
1. **🔄 Resumable Downloads**: Automatically resumes interrupted downloads on subsequent runs (if not using `--stream`)
1. **⚙️ CLI Configuration**: Pass config via command-line args
1. **🚀 FFI**: For even faster CSV processing

## Installation

Requires [Deno](https://deno.land/) 1.40+

## Quick Start

### 1. Populate Database

Download and populate local sqlite DB with the Gaia DR3 catalog:

```bash
# Default settings (10 parallel downloads)
# Will download all 3300+ ~200MB file to populate the local DB
deno task populate

# Custom database location with 20 parallel downloads
deno task populate --db-path ./data/gaia.db --parallel 20

# Test 3 concurrent downloads, limiting to only 5 files
deno task populate --file-limit 5 --parallel 3

# Stream downloads directly into local DB
deno task populate --stream

# Populate DB with Gaia DR3 data, using C FFI for faster CSV processing, and debug output (Rust FFI available via `--rust-ffi`)
deno task populate:gaia --c-ffi --log-level debug
```

### 2. Population Stats

```bash
# Get stats on database population progress
deno task stats

# View statistics on a specific database
deno task stats --db-path ./mydb.db
```

### 3. CLI Queries

```bash
# Query for 10 results around M45
deno run --allow-net --allow-sys --allow-read --allow-write --allow-env --allow-ffi src/cli.ts query --db-path ./gaia.db --ra 56.75 --dec 24.12 --radius 0.5 --limit 10

```

## CLI Reference

### Commands

- `populate` - Download and populate the database with Gaia DR3 data, 2MASS crossmatch, and 2MASS magnitudes (in order)
  - `populate:gaia` - Download and populate the database with Gaia DR3 data only
  - `populate:tmass-xmatch` - Download and populate the database 2MASS crossmatch only
  - `populate:tmass` - Download and populate the database 2MASS magnitudes only
- `query` - Show database statistics
- `stats` - Show database statistics

## Performance

On my M3 Max Macbook Pro these are the running times for population. These are largely bottlenecked by your network speed when downloading these files. Some may be slow, even on fast connections. Tests for individual CSV parsing can be found in [README.md](./ffi/README.md).

- `populate:gaia` — ~14hours (Could be faster using C/Rust FFI)
- `populate:tmass-xmatch` — ~5.5hours
- `populate:tmass` — ~5hours

## Usage as Library

```typescript
import { createGaia, DEFAULT_CONFIG } from "./mod.ts";

const gaia = createGaia({
  // Create Gaia instance, passing pre-populated local DB.
  databasePath: "./gaiaoffline.db",
  photometryOutput: "magnitude",
  magnitudeLimit: [-3, 20],
});

// Cone search around RA=45°, Dec=6°, radius=0.2°
const results = gaia.coneSearch(45, 6, 0.2);
console.log(`Found ${results.length} stars`);

for (const star of results.slice(0, 5)) {
  console.log({
    source_id: star.source_id,
    ra: star.ra,
    dec: star.dec,
    g_mag: star.phot_g_mean_mag,
  });
}

gaia.close();
```

## Configuration

Default columns stored:

```
source_id, ra, dec, parallax, pmra, pmdec, radial_velocity,
phot_g_mean_flux, phot_bp_mean_flux, phot_rp_mean_flux,
teff_gspphot, logg_gspphot, mh_gspphot
```

Default magnitude limit: 16 (stores stars brighter than magnitude 16)

## Contributing

This is a port of the [gaiaoffline](https://github.com/jpdeleon/gaiaoffline) Python library.

## License

MIT License (same as original Python version)
