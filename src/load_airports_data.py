import polars as pl
from services.db_connect import ps_connect
AIRPORTS_FILE_PATH = '../data/airports.csv'


def read_file_airports():
    df_airports = pl.read_csv(AIRPORTS_FILE_PATH)
    airports_with_icao_code = df_airports.filter((pl.col("icao_code" ).is_not_null())&
    (pl.col("icao_code") != ""))
    df_final = airports_with_icao_code.select([
        pl.col("type"),
        pl.col("name"),
        pl.col("latitude_deg"),
        pl.col("longitude_deg"),
        pl.col("elevation_ft"),
        pl.col("iso_country"),
        pl.col("iso_region"),
        pl.col("icao_code"),
        pl.col("iata_code")
    ])
    return df_final

def insert_data(df):
    conn = ps_connect()
    cursor = conn.cursor()
    # insert data into the database
    data = df.rows()
    query = """
    INSERT INTO airports (
        type,
        name,
        latitude_deg,
        longitude_deg,
        elevation_ft,
        iso_country,
        iso_region,
        icao_code,
        iata_code
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (icao_code)
    DO NOTHING;"""
    cursor.executemany(query, data)
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    df_final = read_file_airports()
    insert_data(df_final)

    

    