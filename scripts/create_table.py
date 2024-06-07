import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import psycopg2
from src.constants import DB_FIELDS, POSTGRES_DB, POSTGRES_URL, POSTGRES_PASSWORD, POSTGRES_USER

# Database connection parameters (through Docker)
# Connect to the database
conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_URL)
cur = conn.cursor()


def try_execute_sql(sql: str) -> None:
    """Function which takes in a sql string for execution.

    Args:
        sql (str): Input SQL string.
    """
    try:
        cur.execute(sql)
        #Commit any pending transactions to the database.
        conn.commit()
        print(f"Executed table creation successfully")
    except Exception as e:
        print(f"Couldn't execute table creation due to exception: {e}")
        # Good practice to rollback after to ensure proper reset.
        conn.rollback()


def create_table() -> None:
    """Creates the CNA table and its columns, witb the first field serving as PRIMARY KEY.
    """

    # Create table sql command string
    create_table_sql = f"""
    CREATE TABLE CNA (
        {DB_FIELDS[0]} text PRIMARY KEY,
    """

    # Extend sql query statement for other fields with text type
    for field in DB_FIELDS[1:-1]:
        column_sql = f"{field} text, \n"
        create_table_sql += column_sql

    # Terminating string  
    create_table_sql += f"{DB_FIELDS[-1]} text \n" + ");"
    try_execute_sql(create_table_sql)

    # Close pointer
    cur.close()
    # Close connection to DB
    conn.close()


if __name__ == "__main__":
    create_table()
