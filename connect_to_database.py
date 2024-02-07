import pypyodbc as odbc
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
import pandas as pd
from dotenv import load_dotenv
import os
import time


# connect to database
# specify server and DB name
server = "spotifyrockdb.database.windows.net"
database = "SpotifyRockDB"

# load credentials
try:
    load_dotenv()
    password = os.getenv("PASSWORD")
    #password = os.environ["PASSWORD"]
    # set connection string
    connection_string = (
    'Driver={ODBC Driver 18 for SQL Server};'
    'Server=tcp:' + server + ',1433;'
    'Database=' + database + ';'
    'Uid=sqladmin;'
    'Pwd=' + password + ';'
    'Encrypt=yes;'
    'TrustServerCertificate=no;'
    'Connection Timeout=600;')
    
except Exception as e:
    print("An exception occurred: Database PASSWORD not found")
                                                                    


# connect to database using pyodbc (without SQLAlchemy)
def database_connection(connection_string=connection_string, max_retries=10, retry_delay=3):
    """Connect to SQL Server using pyodbc

    Args:
        connection_string (string): The connection string to the database
        max_retries (int, optional): Max number of retries. Defaults to 10.
        retry_delay (int, optional): Number of seconds to delay between retries. Defaults to 3.

    Returns:
        odbc connection: The connection to the database
    """
    attempts = 0
    while attempts < max_retries:
        try:
            conn = odbc.connect(connection_string)
            return conn
        except Exception as e:
            #logging.error(f"An exception occurred: connect with DB failed (Attempt {attempts + 1}/{max_retries})", exc_info=False)
            attempts += 1
            time.sleep(retry_delay)
            print(f"Failed to connect to the database. Exception raised: {e}")
    return None


# set SQLAlchemy engine
def set_engine(conn=database_connection(), max_retries=5, retry_delay=3):
    """Creates a SQLAlchemy engine using a preexisting connection

    Args:
        conn (odbc deonnection): The connection to the database
        max_retries (int, optional): Max number of retries. Defaults to 5.
        retry_delay (int, optional): Number of seconds to delay between retries. Defaults to 3.

    Returns:
        SQLAlchemy engine: The SQLAlchemy engine
    """
    attempts = 0
    while attempts < max_retries:
        try:
            engine = create_engine("mssql+pyodbc://", poolclass=StaticPool, creator=lambda: conn)
            return engine
        except Exception as e:
            print(f"An exception occurred: SQLAlcehmy engine error (Attempt {attempts + 1}/{max_retries})")
            attempts += 1
            time.sleep(retry_delay)

    print(f"Failed to create the SQLAlchemy engine after {max_retries} attempts. Exception raised: {e}")
    return None



def get_data_from_db(sql, engine=set_engine()):
    """Get data from database using a SQL query

    Args:
        sql (string): The SQL query
        engine (SQL Alchemy engine, optional): Defaults to set_engine(database_connection(connection_string)).

    Returns:
        pandas.DataFrame: The data from the database
    """
    try:
        data = pd.read_sql(sql, engine)
        engine.dispose()
        return data
    except Exception as e:
        print(f"An exception occurred: SQL query failed. Exception raised: {e}")
        engine.dispose()
        return None
    
    
# track time to run the query
start_time = time.time()
tracks_sql = '''SELECT  *
  FROM tracks_table t JOIN albums_table a ON t.album_id=a.album_id
  JOIN artists_table ar ON a.artist_id = ar.artist_id
  JOIN tracks_features_table tf ON t.track_id = tf.track_id
  JOIN tracks_popularity_table tp ON t.track_id = tp.track_id; 
'''
data = get_data_from_db(tracks_sql)
print(data.shape)
end_time = time.time()
print(f"Time to run the query: {end_time - start_time} seconds")