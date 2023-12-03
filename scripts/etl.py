import clickhouse_connect
import sqlite3
import os
from dotenv import load_dotenv

# Loading the environment variables from the .env file
load_dotenv()

CLICKHOUSE_CLOUD_HOSTNAME = os.getenv('CLICKHOUSE_CLOUD_HOSTNAME')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT'))
CLICKHOUSE_USERNAME = os.getenv('CLICKHOUSE_USERNAME')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
DATABASE_NAME = os.getenv('DATABASE_NAME')

def run_script():
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_CLOUD_HOSTNAME, port=CLICKHOUSE_PORT, username=CLICKHOUSE_USERNAME, password=CLICKHOUSE_PASSWORD)

    QUERY = '''
        SELECT
            DATE_FORMAT(pickup_date, '%Y-%m') AS month,
            AVG(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN 1 ELSE 0 END) AS sat_mean_trip_count,
            AVG(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN fare_amount END) AS sat_mean_fare_trip,
            AVG(CASE WHEN DAYOFWEEK(pickup_date) = 7 THEN TIMESTAMPDIFF('SECOND', pickup_datetime, dropoff_datetime) END) AS sat_mean_duration_per_trip,
            AVG(CASE WHEN DAYOFWEEK(pickup_date) = 1 THEN 1 ELSE 0 END) AS sun_mean_trip_count,
            AVG(CASE WHEN DAYOFWEEK(pickup_date) = 1 THEN fare_amount END) AS sun_mean_fare_trip,
            AVG(CASE WHEN DAYOFWEEK(pickup_date) = 1 THEN TIMESTAMPDIFF('SECOND', pickup_datetime, dropoff_datetime) END) AS sun_mean_duration_per_trip
        FROM
            tripdata
        WHERE
            pickup_date BETWEEN '2014-01-01' AND '2016-12-31'
        GROUP BY
            DATE_FORMAT(pickup_date, '%Y-%m')
        ORDER BY
            month
    '''

    result = client.query(QUERY)

    sqlite_conn = sqlite3.connect(DATABASE_NAME)
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute('''
    CREATE TABLE IF NOT EXISTS moniepoint_metrics (
        month TEXT,
        sat_mean_trip_count REAL,
        sat_mean_fare_trip REAL,
        sat_mean_duration_per_trip REAL,
        sun_mean_trip_count REAL,
        sun_mean_fare_trip REAL,
        sun_mean_duration_per_trip REAL
    )
    ''')

    for row in result.result_rows:
        sqlite_cursor.execute('''
        INSERT INTO moniepoint_metrics VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', row)

    sqlite_conn.commit()
    sqlite_conn.close()

    print("Query results written to SQLite database successfully. You rock!")
