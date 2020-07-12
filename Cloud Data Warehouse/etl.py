import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""Loading data from S3 bucket to Redshift"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""INSERT data from staging table to FACT AND DIMENSION TABLE"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print('Connecting to redshift')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to redshift')
    cur = conn.cursor()
    
    print('Loading staging tables')
    load_staging_tables(cur, conn)
    
    print('INSERTING from staging')
    insert_tables(cur, conn)
    
    print('ETL Process Completed')
    conn.close()


if __name__ == "__main__":
    main()