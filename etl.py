"""
Import SQL queries from sql_queries, connect to host and run
queries to transfer the data and fill tables 
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    # Run queries to copy data from S3 to staging tables
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    # Run queries to fill tables
    for query in insert_table_queries:
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
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()