import polars as pl
AIRPORTS_FILE_PATH = '../data/airports.csv'
import create_airports_table
from services.db_connect import ps_connect

def create_aircraft_types_seats_table():
    conn = ps_connect()
    cursor = conn.cursor()
    cursor.execute(""" 
    CREATE TABLE IF NOT EXISTS aircraft_types_seats (
        id SERIAL PRIMARY KEY,
        type TEXT NOT NULL UNIQUE,
        model TEXT NOT NULL,
        seats_min INTEGER NOT NULL,
        seats_max INTEGER NOT NULL
    );
    """)
    conn.commit()
    cursor.close()  
    conn.close()


def check_table():
    conn = ps_connect()
    cursor = conn.cursor()
    cursor.execute("""INSERT INTO aircraft_types_seats
                   (type, model, seats_min, seats_max)
                    VALUES ('73H', 'Boeing 737-800', 162, 189) ON CONFLICT (type) DO NOTHING;""")
    conn.commit()
    cursor.execute("SELECT * FROM aircraft_types_seats;")
    rows = cursor.fetchall()
    print("Data in aircraft_types_seats:")
    print(rows)
    cursor.close()
    conn.close()

    
def delete_rows():
    conn = ps_connect()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM aircraft_types_seats;")
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_aircraft_types_seats_table()
    check_table()
    delete_rows()