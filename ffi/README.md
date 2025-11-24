# FFI Bindings

CSV parsing on the Gaia DR3 dataset is pretty slow in Deno. To improve the speed, we can use other languages specifically for CSV parsing, called from Deno via FFI. This gives some performance gains while also being able to use TypeScript. Some initial test results can be found below, reading a gzipped csv file from [the index](https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/).

### Initial tests on MacBook Pro M3 Max
- Pure C	                  1.39s	  372K rows/sec
- Deno + C FFI	              2.92s	  178K rows/sec
- Pure Rust	                  3.3s	  156K rows/sec
- Deno + Rust FFI	          3.84s	  137K rows/sec
- Go                          4.4s	  119K rows/sec
- Zig (gunzip + parse):       5.74s   90K rows/sec
- Python (pandas)	          6.4s	  81K rows/sec
- Deno (native)	              13.9s	  40K rows/sec
- Zig (native decompress):    21.17s  24K rows/sec (sus)

### Testing yourself

Download a file from [the index](https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/) and label it `test.csv.gz` inside the `tests` folder.

Run any of the source files
