import pandas as pd
from config.kafka import init_consumer
from dotenv import load_dotenv
import logging
from config.logging import setup_logging
from config.database import engine
from sqlalchemy import text

setup_logging()
load_dotenv()

consumer = init_consumer('weather_raw_data')


def normalized_weather_dat(source, weather_data):
    # OpenWeatherMap source
    if source == 'openweathermap':
        return {
            'location': weather_data.get('name'),
            'country': weather_data.get('sys', {}).get('country'),
            'localtime': weather_data.get('dt'),
            'temperature': weather_data.get('main', {}).get('temp'),
            'feels_like': weather_data.get('main', {}).get('feels_like'),
            'condition': weather_data.get('weather', [{}])[0].get('main'),
            'humidity': weather_data.get('main', {}).get('humidity'),
            'pressure': weather_data.get('main', {}).get('pressure'),
            'gust': weather_data.get('wind', {}).get('gust'),
            'api_source': 'openweathermap'
        }

    # WeatherAPI source
    elif source == 'weatherapi':
        return {
            'location': weather_data.get('location', {}).get('name'),
            'country': weather_data.get('location', {}).get('country'),
            'localtime': weather_data.get('location', {}).get('localtime'),
            'temperature': weather_data.get('current', {}).get('temp_c'),
            'feels_like': weather_data.get('current', {}).get('feelslike_c'),
            'condition': weather_data.get('current', {}).get('condition', {}).get('text'),
            'humidity': weather_data.get('current', {}).get('humidity'),
            'pressure': weather_data.get('current', {}).get('pressure_mb'),
            'gust': weather_data.get('current', {}).get('gust_mph'),
            'api_source': 'weatherapi'
        }
    else:
        return None


def save_to_postgres(weather_batch_data):
    try:
        df = pd.DataFrame(weather_batch_data)

        if df.empty:
            logging.info("No rows to insert")
            return

        for col in ['temperature', 'feels_like', 'humidity', 'pressure', 'gust']:
            df[col] = df[col].astype(str)

        records = df.where(pd.notna(df), None).to_dict(orient='records')

        insert_sql = text("""
        INSERT INTO raw.raw_weather_data (
            location, country, "localtime", temperature, feels_like, "condition",
            humidity, pressure, gust, api_source
        ) VALUES (
            :location, :country, :localtime, :temperature, :feels_like, :condition,
            :humidity, :pressure, :gust, :api_source
        )
        """)

        chunksize = 1000
        try:
            with engine.begin() as conn:
                for start in range(0, len(records), chunksize):
                    conn.execute(insert_sql, records[start:start + chunksize])
            logging.info(f"{len(records)} rows inserted into PostgreSQL")
        except Exception as e:
            logging.error(f"Database error: {e}")
            raise
    except Exception as e:  # Python general error
        logging.error(f"Unexpected Error: {e}")


if __name__ == "__main__":
    batch = []
    logging.info("Kafka consumer started...")

    for msg in consumer:
        key_list = msg.key.split('-')
        print(f"Processing message: {msg.value}")
        item = normalized_weather_dat(key_list[0], msg.value)
        batch.append(item)
        if len(batch) >= 10:
            save_to_postgres(batch)
            consumer.commit()
            batch.clear()
