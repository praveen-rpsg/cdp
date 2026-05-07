import psycopg
conn = psycopg.connect('host=127.0.0.1 port=5432 user=postgres password=Raghav_1174 dbname=cdp_meta')
with conn.cursor() as cur:
    cur.execute("SELECT definition FROM pg_views WHERE viewname = 's_dim_location_master' AND schemaname = 'silver'")
    print(cur.fetchone()[0])
