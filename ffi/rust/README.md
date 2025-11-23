# Rust FFI

## Building

```bash
cargo build --release
```

## How It Works

1. **Rust** (this library) - Parses gzipped CSV files
2. **Deno FFI** - Calls Rust functions from TypeScript
3. **TypeScript** - Filters data and inserts into SQLite

## Architecture

```
TypeScript → FFI Bridge → Rust CSV Parser → Native Libraries
   (Deno)     (libgaia_csv_parser.dylib)    (csv + flate2)
```

## Library Output

- **macOS**: `target/release/libgaia_csv_parser.dylib`
- **Linux**: `target/release/libgaia_csv_parser.so`
- **Windows**: `target/release/gaia_csv_parser.dll`
