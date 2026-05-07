import psycopg2

def check_values():
    try:
        conn = psycopg2.connect(
            dbname="cdp_meta",
            user="cdp",
            password="cdp",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()
        
        query = """
            SELECT class_desc, COUNT(*) 
            FROM s_fact_bill_transactions 
            GROUP BY class_desc 
            ORDER BY COUNT(*) DESC 
            LIMIT 50;
        """
        
        cur.execute(query)
        rows = cur.fetchall()
        
        print("Top 50 Product Classes in the Database:")
        print("-" * 40)
        found_accessories = False
        for row in rows:
            print(f"{row[0]}: {row[1]}")
            if row[0] and "ACCESSORIES" in str(row[0]).upper():
                found_accessories = True
        
        if not found_accessories:
            print("\nWARNING: No values containing 'ACCESSORIES' were found.")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error connecting to DB: {e}")

if __name__ == "__main__":
    check_values()
