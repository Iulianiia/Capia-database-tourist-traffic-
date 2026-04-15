from services.db_connect import ps_connect
import polars as pl
import re
from psycopg2.extras import execute_batch
AVINOR_FLIGHTS_PATH = '../data/avinor_flights.csv'


def sum_seats(config_str):
    # sum the number of seats based on the aircraft configuration string,
    if config_str is None:
        return None
    total = 0
    # if the configuration string contains "VV", 
    # ignore everything after "VV" as it indicates a variable configuration
    vv_index = config_str.find("VV")
    if vv_index != -1:
        config_str = config_str[:vv_index]
    # Use regex to find all occurrences of the pattern (letter followed by number)
    matches = re.findall(r'([CMYJW])(\d+)', config_str)
    for letter, num in matches:
        total += int(num)
    return total

def read_configuration_aircraft_type(df):
    # for each row in the dataframe, sum the number of seats based on 
    # the aircraft configuration and add a new column with the total number of seats
    df = df.with_columns([
        pl.col("aircraft.configuration").map_elements(sum_seats).alias("seats")
    ])
    return df



def read_file_avinor_flights():
    df_avinor_flights = pl.read_csv(AVINOR_FLIGHTS_PATH)

    # convert std and sta to time and date.of.operation to date,
    # and create a new column date_of_arrival 
    # based on the condition if std > sta then date_of_arrival = date_of_operation + 1 day 
    # else date_of_arrival = date_of_operation
    df = df_avinor_flights.with_columns([
        
        pl.col("sta").str.strptime(pl.Time, "%H:%M:%SZ"),
        pl.col("std").str.strptime(pl.Time, "%H:%M:%SZ"),
        pl.col("date.of.operation").str.strptime(pl.Date, "%Y-%m-%dZ")
    ]).rename({
        "date.of.operation": "date_of_departure"
    })
    df = df.with_columns([
        pl.when(pl.col("std") > pl.col("sta"))
        .then(pl.col("date_of_departure") + pl.duration(days=1)).otherwise(pl.col("date_of_departure")).alias("date_of_arrival")
    ])

    df = df.with_columns([
        pl.datetime(
            pl.col("date_of_departure").dt.year(),
            pl.col("date_of_departure").dt.month(),
            pl.col("date_of_departure").dt.day(),
            pl.col("std").dt.hour(),
            pl.col("std").dt.minute(),
        ).alias("date_time_of_departure"),

        pl.datetime(
            pl.col("date_of_arrival").dt.year(),
            pl.col("date_of_arrival").dt.month(),
            pl.col("date_of_arrival").dt.day(),
            pl.col("sta").dt.hour(),
            pl.col("sta").dt.minute(),
        ).alias("date_time_of_arrival"),
    ])
    # filter out rows where aircraft.configuration is null or empty
    # and then read the configuration of the aircraft type 
    # and add a new column with the total number of seats based on the configuration
    df_with_configuration = df.filter(pl.col("aircraft.configuration").is_not_null())
    df_with_configuration_seats = read_configuration_aircraft_type(df_with_configuration)

    df = df.with_columns(
    pl.lit(None).alias("seats")
)


    # concat the two dataframes and prepare the final dataframe to match the database schema
    df_final = pl.concat([df_with_configuration_seats, df]).select([
        pl.col("@id"),
        pl.col("airline.designator"),
        pl.col("flight.number"),
        pl.col("date_time_of_departure"),
        pl.col("departure.station"),
        pl.col("date_time_of_arrival"),
        pl.col("arrival.station"),
        pl.col("aircraft.type"),
        pl.col("aircraft.configuration"),
        pl.col("route"),
        pl.col("seats")
    ]).rename({
        "@id": "id_avinor",
        "airline.designator": "airline_designator_iata",
        "flight.number": "flight_number",
        "departure.station": "departure_airport_iata",
        "arrival.station": "arrival_airport_iata",
        "aircraft.type": "aircraft_type",
        "aircraft.configuration": "aircraft_configuration"
    })

    return df_final

def insert_data_to_db(df_final):
    conn = ps_connect()
    cursor = conn.cursor()
    data = df_final.rows()

   
    query = """
    INSERT INTO avinor_flights (
        id_avinor,
        airline_designator_iata,
        flight_number,
        date_time_of_departure,
        departure_airport_iata,
        date_time_of_arrival,
        arrival_airport_iata,
        aircraft_type,
        aircraft_configuration,
        route,
        seats
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id_avinor)
    DO NOTHING;
    """

    execute_batch(cursor, query, data)
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    df_final = read_file_avinor_flights()
    insert_data_to_db(df_final)