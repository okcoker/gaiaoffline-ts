# How to run:
# python -m venv venv
# source venv/bin/activate
# pip install pandas
# python test.py

import gzip
import time

import pandas as pd

file_path = "./test.csv.gz"

print(f"Reading: {file_path}")

start = time.time()

# Read CSV with pandas
df = pd.read_csv(file_path, comment="#")

duration = time.time() - start

count = len(df)

print(f"\nParsed {count:,} rows in {duration:.2f}s")
print(f"Rate: {int(count / duration):,} rows/sec")
