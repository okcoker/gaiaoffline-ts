# Gaia Offline - Deno

A Deno port of the [`gaiaoffline`](https://github.com/christinahedges/gaiaoffline) Python library for downloading and querying Gaia DR3 star catalog data locally.

## Key Improvements Over Python Version

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

- `populate` - Download and populate the database
- `query` - Run interactive example queries
- `stats` - Show database statistics

## Usage as Library

```typescript
import { createGaia } from "./mod.ts";
import { DEFAULT_CONFIG } from "./config.ts";

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

## Architecture

### Parallel Download + Sequential Insert Pipeline

```
┌─────────────────────────────────────────────────┐
│  Fetch CSV URLs from Gaia archive               │
└───────────────────┬─────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  Batch 1: Download 10 files in parallel         │
└───────────────────┬─────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  Parse CSVs, filter by magnitude                │
└───────────────────┬─────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  Insert into SQLite (sequential, transactional) │
└───────────────────┬─────────────────────────────┘
                    │
                    ▼
         While inserting Batch N,
         download Batch N+1 in parallel
```

## Performance

Expected performance improvements:

- **Download time**: ~6-12x faster with 10-20 parallel downloads
- **Total time**: ~4-6 hours (vs 12+ hours for Python version)
- **Network**: Saturates available bandwidth instead of waiting for sequential downloads

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
