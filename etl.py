import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """Function Copies Files from S3 Source into the AWS Redshif Database

    Args:
        cur (Cursor Object): they are bound to the connection for the entire lifetime and all the commands are executed in the 
                             context of the database session wrapped by the connection.
        conn (Connection String): Create a new database session and return a new connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """Function Inserts data from the S3 3NF Files into the AWS Redshift tables in DEV Database

    Args:
        cur (Cursor Object): they are bound to the connection for the entire lifetime and all the commands are executed in the 
                             context of the database session wrapped by the connection.
        conn (Connection String): Create a new database session and return a new connection object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()