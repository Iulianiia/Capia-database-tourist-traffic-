import sys

from psycopg2 import sql
from db_connect import ps_connect

def delete_rows(table_name):
    conn = ps_connect()
    cursor = conn.cursor()

    query = sql.SQL("DELETE FROM {}").format(
        sql.Identifier(table_name)
    )

    cursor.execute(query)
    conn.commit()

    cursor.close()
    conn.close()
    
if __name__ == "__main__":
    table_name =sys.argv[1]
    delete_rows(table_name)