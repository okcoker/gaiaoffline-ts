// How to run:
// deno run --allow-read test.ts

import { CsvParseStream } from "@std/csv/parse-stream";

const filePath = "./test.csv.gz";

console.log("Reading:", filePath);

const start = performance.now();

const file = await Deno.open(filePath, { read: true });

const csvStream = file.readable
  .pipeThrough(new DecompressionStream("gzip"))
  .pipeThrough(new TextDecoderStream())
  .pipeThrough(
    new CsvParseStream({
      comment: "#",
    }),
  );

let count = 0;
let isFirstRow = true;

for await (const record of csvStream) {
  if (isFirstRow) {
    isFirstRow = false;
    continue; // Skip header
  }
  count++;
}

const duration = (performance.now() - start) / 1000;

console.log(
  `\nParsed ${count.toLocaleString()} rows in ${duration.toFixed(2)}s`,
);
console.log(`Rate: ${Math.round(count / duration).toLocaleString()} rows/sec`);
