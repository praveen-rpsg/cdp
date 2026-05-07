import psycopg
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "postgresql://cdp:cdp@localhost:5432/cdp_meta")

def check_region_codes():
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                print("--- Region Codes in Transactions ---")
                cur.execute("SELECT DISTINCT region FROM silver.s_fact_bill_transactions")
                for row in cur.fetchall():
                    print(f"Code: {row[0]}")
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    check_region_codes()
