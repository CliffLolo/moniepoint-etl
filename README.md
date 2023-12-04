
# Moniepoint ETL
This repository contains an Airflow DAG for orchestrating the execution of an ETL process. The ETL process extracts data from a ClickHouse database, performs necessary transformations, and loads the results into a SQLite database.

## Project Structure
```
moniepoint-etl/
|-- dags/
|   |-- main.py
|-- scripts/
|   |-- etl.py
|-- .env
|-- requirements.txt
|-- README.md
```

- **dags/main.py**: Airflow DAG definition script.
- **scripts/etl.py**: Python script containing the ETL logic.
- **.env**: Configuration file for storing environment variables.
- **requirements.txt**: List of Python dependencies.
- **README.md**: Project documentation file.

## Setup

1. **Clone the repository:**

   ```bash
   git clone git@github.com:CliffLolo/moniepoint-etl.git
   ```

2. **cd into directory:**
    ```bash
    cd moniepoint-etl
    ```

3. **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4. **Create a .env file in the project root and set the following environment variables:**
```
CLICKHOUSE_CLOUD_HOSTNAME=your-clickhouse-hostname
CLICKHOUSE_PORT=your-clickhouse-port
CLICKHOUSE_USERNAME=your-clickhouse-username
CLICKHOUSE_PASSWORD=your-clickhouse-password
DATABASE_NAME=your-sqlite-database-name.db
```
## ETL Logic

The ETL logic is defined in the scripts/etl.py file. It connects to a ClickHouse database, executes a SQL query, and then stores the results in a SQLite database.

## SQL Query
The SQL query extracts aggregated statistics from the tripdata table
```
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
    month;

```

## SQLite Table
The results are stored in a SQLite table named moniepoint_metrics with the following schema:
```
CREATE TABLE IF NOT EXISTS moniepoint_metrics (
    month TEXT,
    sat_mean_trip_count REAL,
    sat_mean_fare_trip REAL,
    sat_mean_duration_per_trip REAL,
    sun_mean_trip_count REAL,
    sun_mean_fare_trip REAL,
    sun_mean_duration_per_trip REAL
);

```

## Screenshots
![Airflow dag screenshot](https://github.com/CliffLolo/aws-glue-workshop/assets/41656028/7000125e-4cee-4f69-9092-c204847658c8)
![Airlfow logs screenshot](https://github.com/CliffLolo/aws-glue-workshop/assets/41656028/d0ef5202-611b-459f-af27-455b1c35b996)
![Database containing data screenshot](https://github.com/CliffLolo/aws-glue-workshop/assets/41656028/168d95fd-a41f-4ab6-92da-101af9e58d92)


## Documentation

[Clickhouse Documentation](https://clickhouse.com/docs)