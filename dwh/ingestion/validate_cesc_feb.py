import csv
import glob
import os

files = sorted(glob.glob("/data/*.tsv"))

print(f"Files found: {len(files)}")
print()

for path in files:
    name = os.path.basename(path)

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        header = next(reader, None)

        if header is None:
            print(f"{name}: EMPTY FILE")
            continue

        rows = []
        for _ in range(3):
            row = next(reader, None)
            if row is None:
                break
            rows.append(row)

    print(f"{name}: header_cols={len(header)}, data_rows_checked={len(rows)}")

    for i, row in enumerate(rows, 1):
        month = row[1] if len(row) > 1 else "<missing>"
        print(f"  row {i}: cols={len(row)}, BILL_AC_MONTH={month}")

    if not rows:
        print("  HEADER ONLY")
