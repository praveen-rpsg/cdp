import psycopg
conn = psycopg.connect('host=127.0.0.1 port=5432 user=cdp password=cdp dbname=cdp_meta')
with conn.cursor() as cur:
    cur.execute("SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'bronze' AND tablename = 'raw_location_master'")
    for row in cur.fetchall():
        print(row)
