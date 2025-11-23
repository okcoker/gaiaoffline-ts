// How to run:
// cargo run --release --bin test

use std::fs::File;
use std::io::BufReader;
use std::time::Instant;
use flate2::read::GzDecoder;
use csv::ReaderBuilder;

fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let chars: Vec<char> = s.chars().collect();

    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(*c);
    }
    result
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_path = "./test.csv.gz";

    println!("Reading: {}", file_path);

    let start = Instant::now();

    // Open gzipped file
    let file = File::open(file_path)?;
    let decoder = GzDecoder::new(file);
    let buf_reader = BufReader::new(decoder);

    // Create CSV reader
    let mut csv_reader = ReaderBuilder::new()
        .comment(Some(b'#'))
        .from_reader(buf_reader);

    let mut count = 0u64;

    // Count rows
    for result in csv_reader.records() {
        let _record = result?;
        count += 1;
    }

    let duration = start.elapsed().as_secs_f64();

    println!("\nParsed {} rows in {:.2}s", format_number(count), duration);
    println!("Rate: {} rows/sec", format_number((count as f64 / duration) as u64));

    Ok(())
}
