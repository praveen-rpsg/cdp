import psycopg
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "postgresql://cdp:cdp@localhost:5432/cdp_meta")

def check_store_codes():
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                print("--- Store Codes in Profiles ---")
                cur.execute("SELECT store_code, COUNT(*) FROM silver_identity.unified_profiles GROUP BY store_code LIMIT 10")
                for row in cur.fetchall():
                    print(f"Store: {row[0]}, Count: {row[1]}")
                
                print("\n--- Store Codes in Location Master ---")
                cur.execute("SELECT store_code, store_zone FROM bronze.raw_location_master LIMIT 10")
                for row in cur.fetchall():
                    print(f"Code: {row[0]}, Zone: {row[1]}")
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    check_store_codes()
