import psycopg
import sys

try:
    conn = psycopg.connect('host=127.0.0.1 port=5432 user=cdp password=cdp dbname=cdp_meta')
    print('SUCCESS: Connected to 127.0.0.1')
    with conn.cursor() as cur:
        cur.execute('SELECT current_user, version()')
        row = cur.fetchone()
        print(f'User: {row[0]}')
        print(f'Version: {row[1]}')
    conn.close()
except Exception as e:
    print(f'FAILURE: {e}')
    sys.exit(1)
