import psycopg2

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="pirat",
    host="localhost",
    port=5432
)

conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE DATABASE capia_flights")
cur.close()
conn.close()

