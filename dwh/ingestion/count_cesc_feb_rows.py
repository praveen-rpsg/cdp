import glob
import os
import pandas as pd

total = 0

for path in sorted(glob.glob("/data/*.tsv")):
    count = sum(
        len(chunk)
        for chunk in pd.read_csv(
            path,
            sep="\t",
            dtype=str,
            keep_default_na=False,
            na_values=[],
            chunksize=50000,
            engine="c",
        )
    )
    print(f"{os.path.basename(path)} rows={count}")
    total += count

print(f"TOTAL DATA ROWS: {total}")
