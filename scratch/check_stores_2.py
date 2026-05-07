import psycopg
import os

def check():
    try:
        # Use hostaddr to be explicit
        conn = psycopg.connect("host=127.0.0.1 port=5432 user=cdp password=cdp dbname=cdp_meta")
        with conn.cursor() as cur:
            print("Connected!")
            cur.execute("SELECT store_code, COUNT(*) FROM silver_identity.unified_profiles GROUP BY store_code")
            for row in cur.fetchall():
                print(f"Store: {row[0]}, Count: {row[1]}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check()
