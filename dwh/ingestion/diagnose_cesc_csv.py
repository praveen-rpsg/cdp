import csv

path = "/data/cih/CESC_1.csv"

print("Checking:", path)

with open(path, "r", encoding="utf-8", newline="") as f:
    reader = csv.reader(f, strict=True)
    records = 0

    try:
        for row in reader:
            records += 1
            if records % 100000 == 0:
                print(f"Records OK: {records:,}")

        print(f"SUCCESS - {records:,} records parsed")

    except csv.Error as e:
        print(f"CSV ERROR: {e}")
        print(f"CSV records parsed before failure: {records:,}")
        print(f"Physical line reported by parser: {reader.line_num:,}")
