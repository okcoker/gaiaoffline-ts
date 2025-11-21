# Gaia Offline - Deno

A high-performance Deno port of the [`gaiaoffline`](https://github.com/christinahedges/gaiaoffline) Python library for downloading and querying Gaia DR3 star catalog data locally.

## Key Improvements Over Python Version

1. **⚡ Parallel Downloads**: Downloads 10-50 CSV files simultaneously (configurable)
2. **🔄 Resumable Downloads**: Automatically resumes interrupted downloads
3. **⚙️ CLI Configuration**: Pass config via command-line args instead of hardcoded paths
4. **🚀 Better Performance**: TypeScript/Deno performance benefits
5. **📊 Real-time Progress**: Live progress bars and statistics

## Installation

Requires [Deno](https://deno.land/) 1.40+

```bash
# Clone or download this directory
cd gaiaoffline-ts

# Make executable (optional)
chmod +x mod.ts
```

## Quick Start

### 1. Populate Database

Download and populate the Gaia DR3 catalog:

```bash
# Default settings (10 parallel downloads)
deno task populate

# Custom database location with 20 parallel downloads
deno task populate --db-path /data/gaia.db --parallel 20

# Test with limited files
deno task populate --file-limit 5 --parallel 3
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

### Options

```
-d, --db-path       Path to SQLite database (default: ./gaiaoffline.db)
-p, --parallel      Number of parallel downloads (default: 10, max: 50)
-m, --mag-limit     Magnitude limit for filtering (default: 16)
-l, --log-level     Log level: DEBUG, INFO, WARN, ERROR (default: INFO)
--no-resume         Disable resume capability for downloads
--columns           Comma-separated list of columns to store
--file-limit        Limit number of files to download (for testing)
```

## Usage as Library

```typescript
import { createGaia } from "./mod.ts";
import { DEFAULT_CONFIG } from "./config.ts";

// Create Gaia instance
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
