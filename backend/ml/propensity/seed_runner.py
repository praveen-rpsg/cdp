"""Run seed_test_data.sql via psycopg — no psql needed."""
import os
import psycopg

conn = psycopg.connect(
    host=os.getenv("PG_HOST", "localhost"),
    port=os.getenv("PG_PORT", "5432"),
    dbname=os.getenv("PG_DB", "cdp_meta"),
    user=os.getenv("PG_USER", "cdp"),
    password=os.getenv("PG_PASSWORD", "cdp"),
)

sql = open("backend/ml/propensity/seed_test_data.sql").read()

with conn.cursor() as cur:
    cur.execute(sql)

with conn.cursor() as cur:
    cur.execute("""
        SELECT 'spencers' AS brand, COUNT(*) AS rows
          FROM silver_reverse_etl.customer_propensity_scores_spencers
        UNION ALL
        SELECT 'nbl', COUNT(*)
          FROM nb_silver_reverse_etl.customer_propensity_scores_nbl
    """)
    for row in cur.fetchall():
        print(row)

conn.commit()
conn.close()
print("Done.")
