/**
 * Deno FFI bindings for C CSV parser
 */

const libPath = Deno.build.os === "darwin"
  ? "./c-csv/libgaia_csv_parser.dylib"
  : Deno.build.os === "windows"
  ? "./c-csv/gaia_csv_parser.dll"
  : "./c-csv/libgaia_csv_parser.so";

const lib = Deno.dlopen(libPath, {
  parse_gzipped_csv: {
    parameters: ["pointer", "pointer", "usize"],
    result: "pointer",
  },
  free_string: {
    parameters: ["pointer"],
    result: "void",
  },
});

const encoder = new TextEncoder();

/**
 * Parse a gzipped CSV file using C
 */
export async function parseGzippedCsvC(
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

  // Call C function
  const resultPtr = lib.symbols.parse_gzipped_csv(
    filePathPtr,
    columnsJsonPtr,
    BigInt(chunkSize),
  );

  if (resultPtr === null) {
    throw new Error("Failed to parse CSV file in C");
  }

  // Read the result string
  const resultView = new Deno.UnsafePointerView(resultPtr);
  const resultCString = resultView.getCString();

  // Parse JSON result
  const records = JSON.parse(resultCString);

  // Free the memory allocated by C
  lib.symbols.free_string(resultPtr);

  return records;
}

/**
 * Close the library (cleanup)
 */
export function closeCLib() {
  lib.close();
}
