import psycopg
import json

conn = psycopg.connect('host=127.0.0.1 port=5432 user=cdp password=cdp dbname=cdp_meta')
with conn.cursor() as cur:
    cur.execute("SELECT name, rules FROM segments WHERE name ILIKE '%At Risk High Spend%'")
    row = cur.fetchone()
    if row:
        print(f"Name: {row[0]}")
        print(f"Rules: {json.dumps(row[1], indent=2)}")
    else:
        print("Segment not found.")
