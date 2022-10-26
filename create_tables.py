"""
Drops and creates tables.

- Run this file to reset tables before each time the ETL scripts are run.
"""
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Create and connect to the sparkifydb.
    
    :return: the connection and cursor to sparkifydb
    """
    # connect to default database
    conn = psycopg2.connect("""host=127.0.0.1 dbname=studentdb 
                               user=student password=student""")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("""CREATE DATABASE sparkifydb 
                   WITH ENCODING 'utf8' TEMPLATE template0""")
    # close connection to default database
    conn.close()    
    # connect to sparkify database
    conn = psycopg2.connect("""host=127.0.0.1 dbname=sparkifydb 
                               user=student password=student""")
    cur = conn.cursor()
    return cur, conn


def drop_tables(cur, conn):
    """
    Drop each table using the queries in `drop_table_queries` list.
    
    :param cur: cursor object
    :param conn: connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create each table using the queries in `create_table_queries` list.
    
    :param cur: cursor object
    :param conn: connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Drop (if exists) and Create the sparkify database."""
    # establishe connection with db and get cursor
    cur, conn = create_database()
    # drop all the tables.
    drop_tables(cur, conn)
    # create all tables needed
    create_tables(cur, conn)
    # close the connection
    conn.close()


if __name__ == "__main__":
    main()
