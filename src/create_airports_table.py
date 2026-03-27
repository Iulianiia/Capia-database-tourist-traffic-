import polars as pl
AIRPORTS_FILE_PATH = '../data/airports.csv'
from services.db_connect import ps_connect

def create_airports_table():
    conn = ps_connect()
    cursor = conn.cursor()
    try:
        cursor.execute("CREATE TYPE airports_type AS ENUM ('balloonport', 'large_airport', 'medium_airport', 'seaplane_base','small_airport', 'heliport', 'closed');")
    except Exception:
        conn.rollback()
    
    
    cursor.execute(""" 
    CREATE TABLE IF NOT EXISTS airports (
        id SERIAL PRIMARY KEY,
        type airports_type NOT NULL,
        name TEXT NOT NULL,
        latitude_deg FLOAT NOT NULL,
        longitude_deg FLOAT NOT NULL,
        elevation_ft INTEGER,
        iso_country TEXT NOT NULL,
        iso_region TEXT NOT NULL,
        icao_code TEXT NOT NULL UNIQUE,
        iata_code TEXT UNIQUE
    );
    """)
    conn.commit()
    cursor.close()  
    conn.close()


def check_table():
    conn = ps_connect()
    cursor = conn.cursor()
    cursor.execute("""INSERT INTO airports 
                   (type, name, latitude_deg, longitude_deg, elevation_ft,
                    iso_country, iso_region, icao_code, iata_code)
                    VALUES ('large_airport', 'Oslo Airport', 60.1939, 11.1004, 681, 'NO', 'NO-03', 'EGNM', 'OSL');""")
    conn.commit()
    cursor.execute("SELECT * FROM airports;")
    rows = cursor.fetchall()
    print("Data in airports:")
    print(rows)
    cursor.close()
    conn.close()

    
def delete_rows():
    conn = ps_connect()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM airports;")
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_airports_table()
    check_table()
    delete_rows()