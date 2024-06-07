import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import psycopg2
from src.constants import DB_FIELDS, POSTGRES_DB, POSTGRES_URL, POSTGRES_PASSWORD, POSTGRES_USER

# Database connection parameters (through Docker)
# Connect to the database
conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_URL)
cur = conn.cursor()


def try_execute_sql(sql: str):
    try:
        cur.execute(sql)
        conn.commit()
        print(f"Executed table creation successfully")
    except Exception as e:
        print(f"Couldn't execute table creation due to exception: {e}")
        conn.rollback()


def create_table():
    """
    Creates the CNA table and its columns.
    """
    create_table_sql = f"""
    CREATE TABLE CNA (
        {DB_FIELDS[0]} text PRIMARY KEY,
    """
    for field in DB_FIELDS[1:-1]:
        column_sql = f"{field} text, \n"
        create_table_sql += column_sql

    create_table_sql += f"{DB_FIELDS[-1]} text \n" + ");"
    try_execute_sql(create_table_sql)

    cur.close()
    conn.close()


if __name__ == "__main__":
    create_table()
