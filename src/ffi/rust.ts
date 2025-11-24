/**
 * Deno FFI bindings for Rust CSV parser
 * Lazy-loaded to avoid requiring --allow-ffi unless actually used
 */

import { join } from "@std/path";

const libPath = Deno.build.os === "darwin"
  ? "./ffi/rust/target/release/libgaia_csv_parser.dylib"
  : Deno.build.os === "windows"
  ? "./ffi/rust/target/release/gaia_csv_parser.dll"
  : "./ffi/rust/target/release/libgaia_csv_parser.so";

let lib:
  | Deno.DynamicLibrary<{
    parse_gzipped_csv: {
      parameters: ["pointer", "pointer", "usize"];
      result: "pointer";
    };
    free_string: {
      parameters: ["pointer"];
      result: "void";
    };
  }>
  | null = null;

/**
 * Lazy-load the Rust library (only loads once)
 */
function getRustLib() {
  if (!lib) {
    const fullPath = join(Deno.cwd(), "../../", libPath);
    lib = Deno.dlopen(fullPath, {
      parse_gzipped_csv: {
        parameters: ["pointer", "pointer", "usize"],
        result: "pointer",
      },
      free_string: {
        parameters: ["pointer"],
        result: "void",
      },
    });
  }
  return lib;
}

const encoder = new TextEncoder();

/**
 * Parse a gzipped CSV file using Rust
 */
export function parseGzippedCsvRust(
  filePath: string,
  columnsToKeep: string[],
  chunkSize = 100000,
): Promise<Array<Record<string, unknown>>> {
  // Convert strings to C strings (null-terminated)
  const filePathBytes = encoder.encode(filePath + "\0");
  const columnsJson = JSON.stringify(columnsToKeep);
  const columnsJsonBytes = encoder.encode(columnsJson + "\0");

  // Allocate memory for the input strings
  const filePathPtr = Deno.UnsafePointer.of(filePathBytes);
  const columnsJsonPtr = Deno.UnsafePointer.of(columnsJsonBytes);

  // Get the library (loads on first call)
  const rustLib = getRustLib();

  // Call Rust function
  const resultPtr = rustLib.symbols.parse_gzipped_csv(
    filePathPtr,
    columnsJsonPtr,
    chunkSize,
  );

  if (resultPtr === null) {
    throw new Error("Failed to parse CSV file in Rust");
  }

  // Read the result string
  const resultView = new Deno.UnsafePointerView(resultPtr);
  const resultCString = resultView.getCString();

  // Parse JSON result
  const records = JSON.parse(resultCString);

  // Free the memory allocated by Rust
  rustLib.symbols.free_string(resultPtr);

  return records;
}

/**
 * Close the library (cleanup)
 */
export function closeRustLib() {
  if (lib) {
    lib.close();
    lib = null;
  }
}
