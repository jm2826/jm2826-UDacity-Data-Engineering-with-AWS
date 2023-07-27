
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Function drops tables in Redshift Database

    Args:
        cur (Cursor Object): they are bound to the connection for the entire lifetime and all the commands are executed in the 
                             context of the database session wrapped by the connection.
        conn (Connection String): Create a new database session and return a new connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Function creates tables in Redshift Database

    Args:
        cur (Cursor Object): they are bound to the connection for the entire lifetime and all the commands are executed in the 
                             context of the database session wrapped by the connection.
        conn (Connection String): Create a new database session and return a new connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Establish a connection with your Amazon Redshift Cluster use the project_dwh.cfg file.
    Calls drop_tables and create_tables functions."""
    
    config = configparser.ConfigParser()
    config.read("project_dwh.cfg")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config["CLUSTER"].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()