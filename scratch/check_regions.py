import psycopg
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "postgresql://cdp:cdp@localhost:5432/cdp_meta")

def check_data():
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            print("--- Regions in Location Master ---")
            cur.execute("SELECT DISTINCT store_business_region FROM silver.s_dim_location_master")
            for row in cur.fetchall():
                print(f"Location Region: {row[0]}")
            
            print("\n--- Region-State mapping in Transactions ---")
            cur.execute("SELECT DISTINCT region_desc FROM silver.s_fact_bill_transactions LIMIT 10")
            for row in cur.fetchall():
                print(f"Transaction Region Desc (State): {row[0]}")

            print("\n--- Sample Transaction count by Region ---")
            cur.execute("""
                SELECT lm.store_business_region, COUNT(*) 
                FROM silver.s_fact_bill_transactions bt
                JOIN silver.s_dim_location_master lm ON lm.store_code = bt.store_code
                GROUP BY 1
            """)
            for row in cur.fetchall():
                print(f"Region: {row[0]}, Count: {row[1]}")

if __name__ == "__main__":
    check_data()
