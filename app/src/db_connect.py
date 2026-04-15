import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()
def ps_connect():
    conn = psycopg2.connect(
        dbname="capia_flights",
        user="postgres",
        password="pirat",
        host="localhost",
    port="5432"
    )
    return conn