import pandas as pd
import glob
import os

cih_dir = os.getenv('CIH_DIR', 'C:\\CIH_parquet')
files = glob.glob(os.path.join(cih_dir, '*.parquet'))

total_rows = 0
print(f"Counting rows in {len(files)} files...")

for f in files:
    try:
        df = pd.read_parquet(f)
        total_rows += len(df)
    except Exception as e:
        print(f"Error reading {f}: {e}")

print(f"TOTAL_PROFILES: {total_rows}")
