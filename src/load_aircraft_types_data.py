import polars as pl
from services.db_connect import ps_connect
AIRCRAFT_TYPES_FILE_PATH = '../data/aircraft_type_list_tbv_noise.csv'
AIRCRAFT_TYPES_IATA_FILE_PATH = '../data/aircrafts_type_list_iata.csv'

def read_file_aircraft_types():
    df_aircraft_types = pl.read_csv(AIRCRAFT_TYPES_FILE_PATH)
    df_types_iata = pl.read_csv(AIRCRAFT_TYPES_IATA_FILE_PATH)
    df1 = df_aircraft_types.with_row_index("id")
    df2 = df_types_iata.with_row_index("id")

    df_final = df1.join(df2, on="id", how="left")
    df_final = df_final.select([
        pl.col("Name"),
        pl.col("Type"),
        pl.col("Group"),
        pl.col("Category"),
        pl.col("Body"),
        pl.col("ICAO Code"),
        pl.col("IATA code")
    ]).rename({
        "Name": "name",
        "Type": "aircraft_type",
        "Group": "aircraft_group",
        "Category": "category",
        "Body": "body",
        "ICAO Code": "icao",
        "IATA code": "iata"
    })
    return df_final

def insert_data(df):
    conn = ps_connect()
    cursor = conn.cursor()
    # insert data into the database
    data = df.rows()
    query = """
    INSERT INTO aircraft_types (
        name,
        aircraft_type,
        aircraft_group,
        category,
        body,
        icao,
        iata
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (aircraft_type)
    DO NOTHING;"""
    cursor.executemany(query, data)
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    df_final = read_file_aircraft_types()
    insert_data(df_final)

    

    