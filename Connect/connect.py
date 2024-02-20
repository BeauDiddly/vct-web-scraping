import psycopg2
from Connect.config import config
from sqlalchemy import create_engine

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        return conn, cur
        
	# execute a statement
        # print('PostgreSQL database version:')
        # cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        # db_version = cur.fetchone()
        # print(db_version)
       
	# close the communication with the PostgreSQL
        # cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    # finally:
    #     if conn is not None:
    #         conn.close()
    #         print('Database connection closed.')
        
def engine():
    engine = None
    params = config()
    host = params["host"]
    db = params["database"]
    user = params["user"]
    password = params["password"]
    engine = create_engine(f"postgresql://{user}:{password}@{host}:5432/{db}")
    return engine