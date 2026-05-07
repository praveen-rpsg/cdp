import psycopg
conn = psycopg.connect('host=127.0.0.1 port=5432 user=postgres password=Raghav_1174 dbname=cdp_meta')
with conn.cursor() as cur:
    cur.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('silver', 'silver_identity')")
    for row in cur.fetchall():
        print(f"{row[0]}.{row[1]}")
