import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops tables as specified in drop_table_queries
    
    Parameters
    ----------
    cur
        Cursor, allows to execute commands in database session
    conn
        Database connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates tables as specified in create_table_queries
    
    Parameters
    ----------
    cur
        Cursor, allows to execute commands in database session
    conn
        Database connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Execute table creation pipeline"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()