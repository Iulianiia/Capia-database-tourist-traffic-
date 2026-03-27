import psycopg2
from services.db_connect import ps_connect


def setup_db():
    conn = ps_connect()
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS avinor_flights (
        id SERIAL PRIMARY KEY,
        id_avinor TEXT NOT NULL UNIQUE,
        airline_designator_iata TEXT NOT NULL,
        flight_number TEXT NOT NULL,
        date_time_of_departure TIMESTAMPTZ NOT NULL,
        departure_airport_iata TEXT NOT NULL,
        date_time_of_arrival TIMESTAMPTZ NOT NULL,
        arrival_airport_iata TEXT NOT NULL,
        aircraft_type TEXT NOT NULL,
        aircraft_configuration TEXT,
        route TEXT NOT NULL,
        seats INTEGER
    );
    """)
    

    conn.commit()
    cursor.close()
    conn.close()

def check_table():
    conn = ps_connect()
    cursor = conn.cursor()
    cursor.execute("""INSERT INTO avinor_flights (id_avinor, airline_designator_iata,
                    flight_number, date_time_of_departure, departure_airport_iata, 
                   date_time_of_arrival, arrival_airport_iata, 
                   aircraft_type, aircraft_configuration, 
                   route, seats)
                    VALUES ('1', 'SAS', '1234', '2023-01-01,10:00:00', 'OSL', '2023-01-01,8:00:00',
                    'CPH', 'Boeing 737', '3-3', 'OSL-CPH', 150);""")
    conn.commit()
    cursor.execute("SELECT * FROM avinor_flights;")
    rows = cursor.fetchall()
    print("Data in avinor_flights:")
    print(rows)
    cursor.close()
    conn.close()

    
def delete_rows():
    conn = ps_connect()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM avinor_flights;")
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    setup_db()
    check_table()
    delete_rows()