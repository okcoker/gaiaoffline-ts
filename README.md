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

### 2. Query Database

```bash
# Run example queries and benchmark
deno task query

# View statistics
deno run --allow-read mod.ts stats --db-path ./gaiaoffline.db
```

## CLI Reference

### Commands

- `populate` - Download and populate the database with Gaia DR3 data, 2MASS crossmatch, and 2MASS magnitudes (in order)
  - `populate:gaia` - Download and populate the database with Gaia DR3 data only
  - `populate:tmass-xmatch` - Download and populate the database 2MASS crossmatch only
  - `populate:tmass` - Download and populate the database 2MASS magnitudes only
- `query` - Run interactive example queries
- `stats` - Show database statistics


## Performance

On my M3 Max Macbook Pro these are the running times for population

- `populate:gaia` — ~14hours (Could be faster using C/Rust FFI)
- `populate:tmass-xmatch` — ~5.5hours
- `populate:tmass` — ~5.5hours

## Usage as Library

```typescript
import { createGaia, DEFAULT_CONFIG } from "./mod.ts";

// Create Gaia instance, passing pre-populated local DB.
const gaia = createGaia("./gaiaoffline.db", DEFAULT_CONFIG, {
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

## Differences from Python Version

1. ✅ CLI-based configuration (no hardcoded `~/.config` paths)
2. ✅ Parallel downloads with configurable concurrency
3. ✅ Resumable downloads using HTTP Range headers
4. ✅ Real-time progress tracking
5. ⏳ 2MASS crossmatch (not yet implemented)
6. ⏳ Full pandas-style data filtering (partially implemented)

## Contributing

This is a port of the [gaiaoffline](https://github.com/jpdeleon/gaiaoffline) Python library.

## License

MIT License (same as original Python version)
