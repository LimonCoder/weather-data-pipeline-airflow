from datetime import datetime
import logging
import numpy as np
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from producer.weather_producer import push_weather_data_to_kafka
import pandas as pd
import time
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger("weather_pipeline")


def to_bd_time(ts):
    try:
        if isinstance(ts, (int, float)):  # Unix timestamp
            return pd.to_datetime(ts, unit='s', utc=True).tz_convert('Asia/Dhaka')
        else:
            return pd.to_datetime(ts).tz_localize('UTC').tz_convert('Asia/Dhaka')
    except Exception as e:
        return ''


country_code = {
    'BD': 'Bangladesh',
}


@dag(
    start_date=datetime(2025, 9, 1),
    schedule="*/15 * * * *",
    catchup=False
)
def weather_pipeline():

    @task
    def ingestion_weather_data():
        try:
            push_weather_data_to_kafka()
            logging.info(f'####(0) ingestion_weather_data')
            return 1
        except Exception as e:
            logging.info(f"Unexpected error in ingestion_weather_data : {e}", exc_info=True)
            return 0

    # Task 1: Extract Weather Data
    @task
    def extract_weather_data():
        hook = PostgresHook(postgres_conn_id='airflow_db')  # connection id
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM raw.raw_weather_data;")
        records = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]

        cursor.close()
        conn.close()

        # Logging
        logging.info(f'####(1) Fetch raw weather data {records}')

        return {'records': records, 'columns': columns}

    @task
    def clean_weather_data(raw_weather_data):
        df = pd.DataFrame(raw_weather_data['records'], columns=raw_weather_data['columns'])
        df['location'] = df['location'].str.split().str[0]
        df['country'] = df['country'].map(country_code).fillna(df['country'])
        df['localtime'] = df['localtime'].apply(to_bd_time)
        df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
        df['humidity'] = pd.to_numeric(df['humidity'], errors='coerce')
        df['pressure'] = pd.to_numeric(df['pressure'], errors='coerce')
        df['gust'] = pd.to_numeric(df['gust'], errors='coerce')
        df.replace('', np.nan, inplace=True)
        df = df.dropna(subset=['country', 'location', 'localtime', 'temperature', 'humidity', 'pressure', 'gust'])

        # Convert timestamps to ISO string for XCom
        df['localtime'] = df['localtime'].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
        df['created_at'] = df['created_at'].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
        df['updated_at'] = df['updated_at'].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

        logging.info(f'####(2) cleaning weather data {df}')

        pg_hook = PostgresHook(postgres_conn_id='airflow_db')
        engine = pg_hook.get_sqlalchemy_engine()

        df.to_sql('clean_weather_data', engine, schema='staging', if_exists='append', index=False)

        return {'records': df.to_dict(orient='records'), 'columns': df.columns.tolist()}

    @task.bash
    def transform_weather_data():
        """
        Runs all dbt models in the project.
        """
        return "cd /opt/airflow/weather_dbt && dbt run --profiles-dir /opt/airflow/weather_dbt"

    @task.bash
    def test_transformed_weather_data():
        """
        Runs all dbt models in the project.
        """
        return "cd /opt/airflow/weather_dbt && dbt test --profiles-dir /opt/airflow/weather_dbt"

    @task.bash
    def snapshot_transformed_weather_data():
        """
        Runs all dbt models in the project.
        """
        return "cd /opt/airflow/weather_dbt && dbt snapshot --profiles-dir /opt/airflow/weather_dbt"

    # Task dependencies
    ingest_task = ingestion_weather_data()
    raw_data = extract_weather_data()

    ingest_task >> raw_data
    clean_data = clean_weather_data(raw_data)
    clean_data >> transform_weather_data() >> test_transformed_weather_data() >> snapshot_transformed_weather_data()



weather_pipeline()
