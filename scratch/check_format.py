import psycopg
import os

# Try connecting without env vars first
conn_str = "host=localhost port=5432 user=cdp password=cdp dbname=cdp_meta"

def check():
    try:
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                print("Profiles Store Codes:")
                cur.execute("SELECT DISTINCT store_code FROM silver_identity.unified_profiles WHERE store_code IS NOT NULL LIMIT 5")
                for row in cur.fetchall():
                    print(f"'{row[0]}'")
                
                print("\nLocation Master Store Codes:")
                cur.execute("SELECT DISTINCT store_code FROM bronze.raw_location_master LIMIT 5")
                for row in cur.fetchall():
                    print(f"'{row[0]}'")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check()
