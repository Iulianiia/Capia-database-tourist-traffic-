import psycopg2
from services.db_connect import ps_connect


def setup_db():
    conn = ps_connect()
    cursor = conn.cursor()
    try:
        cursor.execute("CREATE TYPE traffic_type AS ENUM ('A', 'D');")
    except Exception:
        conn.rollback()

    try:
        cursor.execute("CREATE TYPE traffic_category AS ENUM ('D', 'I');")
    except Exception:
        conn.rollback()


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ssb_airport_monthly_traffic (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        airport_icao_code TEXT NOT NULL,
        arrival_departure traffic_type NOT NULL,
        international_domestic traffic_category NOT NULL,
        flights BIGINT NOT NULL,
        passengers BIGINT NOT NULL,
        seats BIGINT NOT NULL
    );
    """)

    cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS unique_traffic_entry
        ON ssb_airport_monthly_traffic (date, airport_icao_code, arrival_departure, international_domestic);""")
    conn.commit()
    cursor.close()
    conn.close()

def check_table():
    conn = ps_connect()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO ssb_airport_monthly_traffic "
    "(date, airport_icao_code, arrival_departure, international_domestic, flights, passengers, seats) " \
    "VALUES ('2023-01-01', 'OSL', 'D', 'I', 1000, 200000, 250000);")
    conn.commit()
    cursor.execute("SELECT * FROM ssb_airport_monthly_traffic;")
    rows = cursor.fetchall()
    print("Data in ssb_airport_monthly_traffic:")
    print(rows)
    cursor.close()
    conn.close()

    
def delete_rows():
    conn = ps_connect()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM ssb_airport_monthly_traffic;")
    conn.commit()
    cursor.close()
    conn.close()

def delete_table():
    conn = ps_connect()
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS ssb_airport_monthly_traffic;")
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    setup_db()
    check_table()
    delete_rows()
  


