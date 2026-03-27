import psycopg2
from services.db_connect import ps_connect


def setup_db():
    conn = ps_connect()
    cursor = conn.cursor()


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS aircraft_types (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        aircraft_type TEXT NOT NULL UNIQUE,
        aircraft_group TEXT,
        category TEXT NOT NULL,
        body TEXT,
        icao TEXT,
        iata TEXT
    );
    """)

   
    conn.commit()
    cursor.close()
    conn.close()

def check_table():
    conn = ps_connect()
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO aircraft_types (name, aircraft_type, aircraft_group,
                    category, body, icao, iata)
    VALUES ('Airbus A310-200 Passenger', '312', '310',
                    '2j', 'W', 'A310', '312');""")
    conn.commit()
    cursor.execute("SELECT * FROM aircraft_types;")
    rows = cursor.fetchall()
    print("Data in aircraft_types:")
    print(rows)
    cursor.close()
    conn.close()

    
def delete_rows():
    conn = ps_connect()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM aircraft_types;")
    conn.commit()
    cursor.close()
    conn.close()

def delete_table():
    conn = ps_connect()
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS aircraft_types;")
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    setup_db()
    check_table()
    delete_rows()
  

