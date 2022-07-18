"""
Import SQL queries from sql_queries, connect to host and run
queries to create the tables
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    # Run queries to drop existing tables
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    # Run queries to create the tables
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Read config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Open a connection to the host
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Run SQL queries
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()