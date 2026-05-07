import psycopg
conn = psycopg.connect('host=127.0.0.1 port=5432 user=cdp password=cdp dbname=cdp_meta')
with conn.cursor() as cur:
    cur.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_name = 'segments'")
    for row in cur.fetchall():
        print(f"{row[0]}.{row[1]}")
