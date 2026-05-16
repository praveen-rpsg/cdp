#!/usr/bin/env python3
"""Run nbl_fix_unified_profiles.sql via psycopg — avoids psql output buffering issues."""

import sys
import psycopg

DB_URL = "postgresql://cdp:cdp@localhost:5432/cdp_meta"
SQL_FILE = r"C:\cdp_new\nbl_fix_unified_profiles.sql"

def run():
    print("Connecting to cdp_meta ...", flush=True)
    with open(SQL_FILE, "r", encoding="utf-8") as f:
        sql = f.read()

    with psycopg.connect(DB_URL, autocommit=False) as conn:
        conn.add_notice_handler(lambda diag: print(f"  [NOTICE] {diag.message_primary}", flush=True))
        with conn.cursor() as cur:
            print("Executing fix script ...", flush=True)
            cur.execute(sql)
        conn.commit()
        print("COMMIT OK", flush=True)

    print("Done.", flush=True)

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
