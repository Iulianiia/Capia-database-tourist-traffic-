import polars as pl
from services.db_connect import ps_connect
from psycopg2.extras import execute_batch


TRAFFIC_TYPE_MAP = {
    "Arrival": "A",
    "Departure": "D"
}
TRAFFIC_CATEGORY_MAP = {
    "Domestic flights": "D",
    "International flights": "I"
}
def map_traffic_type(value):
    return TRAFFIC_TYPE_MAP.get(value, value)
def map_traffic_category(value):
    return TRAFFIC_CATEGORY_MAP.get(value, value)
SSB_PASSENGERS_FILE_PATH = '../data/ssb_passengers.csv'
SSB_SEATS_FLIGHTS_FILE_PATH = '../data/ssb_seats_and_flights.csv'

def convert_month_year_to_date(month_year_str):
    """
    Converts a string in 'YYYYMmm' format to 'YYYY-MM-01'.

    """
    # Extract year (first 4 characters)
    year = month_year_str[:4]
    
    # Extract month (last 2 characters)
    month = month_year_str[-2:]
    
    # Construct date string with the first day of the month
    date_str = f"{year}-{month}-01"
    return date_str

def read_file_passangers():
    df_passengers = pl.read_csv(SSB_PASSENGERS_FILE_PATH)

    # Keep only rows with Arrival or Departure
    df_passengers = df_passengers.filter(
        (pl.col("passenger group") == "Passengers on board at arrival") |
        (pl.col("passenger group") == "Passengers on board at departure")
    )

    df_passengers = df_passengers.with_columns([
        # Convert month_year to proper date
        pl.col("month").map_elements(convert_month_year_to_date),
        
        # Shorten arrival_departure to 'Arrival' / 'Departure'
        pl.col("passenger group").map_elements(lambda x: "Arrival" if x=="Passengers on board at arrival" else "Departure"),
        
       
    ]).rename({
        "month": "date",
        "passenger group": "aircraft movement",
    })

    print(df_passengers)
    return df_passengers

def read_file_seats_flights():
    df_seats_flights = pl.read_csv(SSB_SEATS_FLIGHTS_FILE_PATH)
    # fix month_year to date
    df_seats_flights = df_seats_flights.with_columns([
        df_seats_flights["month"].map_elements(convert_month_year_to_date)
    ]).rename({"month": "date"})
    return df_seats_flights

def merge_files(df_passengers, df_seats_flights):
    # Concat two dataframse in one
    df_all = pl.concat([df_passengers, df_seats_flights])
    # merge them by pivoting the contents column to get flights, passengers and seats as separate columns
    merged_df = df_all.pivot(
        values= "value",
        index=["Lufthavn", "date","airport", "type of traffic", "domestic/international flights", "aircraft movement"],
        columns="contents"
    )
    # preapare the dataframe to match the database schema
    renamed_df = merged_df.rename({
        "Aircraft movements": "flights",
        "Lufthavn": "airport_code",
        "aircraft movement": "arrival_departure",
        "domestic/international flights": "international_domestic",
        "Passengers": "passengers",
        "Seats": "seats"

    })
    # map arrival_departure and international_domestic to the correct values for the database
    df_final = renamed_df.with_columns([
        pl.col("arrival_departure").map_elements(map_traffic_type),
        pl.col("international_domestic").map_elements(map_traffic_category)
    ])
    print(df_final.row(3))
    return df_final


def insert_data(df):
    conn = ps_connect()
    cursor = conn.cursor()

    data = df.select([
        "date",
        "airport_code",
        "arrival_departure",
        "international_domestic",
        "flights",
        "passengers",
        "seats"
    ]).rows()
    execute_batch(cursor, """
        INSERT INTO ssb_airport_monthly_traffic 
        (date, airport_code, arrival_departure, international_domestic, flights, passengers, seats)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, airport_code, arrival_departure, international_domestic) DO NOTHING
    """, data)

    conn.commit()
    cursor.close()
    conn.close()
if __name__ == "__main__":
    df_passengers = read_file_passangers()
    df_seats_flights = read_file_seats_flights()
    merged_df = merge_files(df_passengers, df_seats_flights)
    insert_data(merged_df)