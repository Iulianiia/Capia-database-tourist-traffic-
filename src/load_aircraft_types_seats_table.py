# load aircraft types seats table
import polars as pl
from services.db_connect import ps_connect
from psycopg2.extras import execute_batch

AIRCRAFT_TYPES_SEATS_FILE_PATH = '../data/aircraft_types_seats.csv'

def read_file_aircraft_types_seats():
    df_aircraft_types_seats = pl.read_csv(AIRCRAFT_TYPES_SEATS_FILE_PATH)
    return df_aircraft_types_seats

def load_aircraft_types_seats_table():
    df_aircraft_types_seats = read_file_aircraft_types_seats()
    conn = ps_connect()
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO aircraft_types_seats (type, model, seats_min, seats_max)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (type) DO NOTHING;
    """
    data_to_insert = (
        df_aircraft_types_seats
        .with_columns([
            pl.col("seats")
            .str.replace_all("–", "-")
            .alias("seats_clean")
        ])
        .with_columns([

            # MIN
            pl.when(pl.col("seats_clean").str.contains("-"))
            .then(
                pl.col("seats_clean")
                .str.extract(r"^(\d+)-", 1)
                .cast(pl.Int64)
            )
            .otherwise(
                pl.col("seats_clean").str.extract(r"(\d+)", 1).cast(pl.Int64)
            )
            .alias("seats_min"),

            # MAX
            pl.when(pl.col("seats_clean").str.contains("-"))
            .then(
                pl.col("seats_clean")
                .str.extract(r"-(\d+)$", 1)
                .cast(pl.Int64)
            )
            .otherwise(
                pl.col("seats_clean").str.extract(r"(\d+)", 1).cast(pl.Int64)
            )
            .alias("seats_max"),
        ])
        .rename({
            "aircraft.type": "type",
            "aircraft.model": "model",
        })
        .select(["type", "model", "seats_min", "seats_max"])
    ).rows()
        
    execute_batch(cursor, """
    INSERT INTO aircraft_types_seats
    (type, model, seats_min, seats_max)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (type) DO NOTHING
""", data_to_insert)
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_aircraft_types_seats_table()
