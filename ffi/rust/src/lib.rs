use std::ffi::{CStr, CString};
use std::fs::File;
use std::io::BufReader;
use std::os::raw::{c_char, c_void};
use std::slice;
use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use serde_json::{json, Value};

/// Parse a gzipped CSV file and return JSON array as a string
///
/// # Safety
/// This function is unsafe because it:
/// - Dereferences raw pointers
/// - Assumes the caller will free the returned string
#[no_mangle]
pub unsafe extern "C" fn parse_gzipped_csv(
    file_path: *const c_char,
    columns_json: *const c_char,
    chunk_size: usize,
) -> *mut c_char {
    // Convert C strings to Rust strings
    let file_path_str = match CStr::from_ptr(file_path).to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    let columns_json_str = match CStr::from_ptr(columns_json).to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    // Parse columns array from JSON
    let columns: Vec<String> = match serde_json::from_str(columns_json_str) {
        Ok(cols) => cols,
        Err(_) => return std::ptr::null_mut(),
    };

    // Parse CSV file
    match parse_csv_internal(file_path_str, &columns, chunk_size) {
        Ok(json_str) => {
            match CString::new(json_str) {
                Ok(c_str) => c_str.into_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        }
        Err(_) => std::ptr::null_mut(),
    }
}

/// Free a string allocated by Rust
#[no_mangle]
pub unsafe extern "C" fn free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        let _ = CString::from_raw(ptr);
    }
}

fn parse_csv_internal(
    file_path: &str,
    columns_to_keep: &[String],
    _chunk_size: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;
    let decoder = GzDecoder::new(file);
    let buf_reader = BufReader::new(decoder);

    let mut csv_reader = ReaderBuilder::new()
        .comment(Some(b'#'))
        .from_reader(buf_reader);

    let headers = csv_reader.headers()?.clone();

    // Find indices of columns to keep
    let column_indices: Vec<usize> = columns_to_keep
        .iter()
        .filter_map(|col| headers.iter().position(|h| h == col))
        .collect();

    let mut records = Vec::new();

    for result in csv_reader.records() {
        let record = result?;
        let mut obj = serde_json::Map::new();

        for &idx in &column_indices {
            if let Some(value) = record.get(idx) {
                let header = &headers[idx];

                // Convert to appropriate type
                let json_value = if value.is_empty() {
                    Value::String(value.to_string())
                } else if header == "source_id" || header == "solution_id" || header == "designation" {
                    Value::String(value.to_string())
                } else {
                    // Try to parse as number
                    match value.parse::<f64>() {
                        Ok(num) => json!(num),
                        Err(_) => {
                            match value.to_lowercase().as_str() {
                                "null" => Value::Null,
                                "true" => Value::Bool(true),
                                "false" => Value::Bool(false),
                                _ => Value::String(value.to_string()),
                            }
                        }
                    }
                };

                obj.insert(header.to_string(), json_value);
            }
        }

        records.push(Value::Object(obj));
    }

    Ok(serde_json::to_string(&records)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_csv() {
        let result = parse_csv_internal(
            "../test.csv.gz",
            &["source_id".to_string(), "ra".to_string(), "dec".to_string()],
            1000,
        );
        assert!(result.is_ok());
    }
}
