import logging
import time
import requests
from config.kafka import init_producer
from config.logging import setup_logging
from dotenv import load_dotenv
import os
import json

load_dotenv()
setup_logging()


API_KEY = os.getenv('WEATHER_API_KEY')
OPEN_WEATHER_MAP_API_KEY = os.getenv('OPEN_WEATHER_MAP_API_KEY')

weather_api_list = {
    'openweathermap': f"https://api.openweathermap.org/data/2.5/weather?appid={OPEN_WEATHER_MAP_API_KEY}&units=metric",
    'weatherapi': f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&aqi=no",
}

CITIES = ["Dhaka", "Chittagong", "Khulna", "Rajshahi", "Barisal", "Sylhet", "Rangpur", "Mymensingh"]

producer = init_producer()


def fetch_weather_from_api(api_url):
    try:
        logging.info(f"Request Weather Api: {api_url}")
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        logging.info(f"Response Weather Api: {json.dumps(response.json())}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for URL: {api_url} | Error: {e}")
        return None


def push_weather_data_to_kafka():
    for api_name, api_url in weather_api_list.items():
        for city in CITIES:
            location_wise_api_endpoint = f"{api_url}&q={city},BD"
            data = fetch_weather_from_api(location_wise_api_endpoint)

            if data:
                key_name = f"{api_name}-{city}"
                try:
                    producer.send('weather_raw_data', key=key_name, value=data)
                    producer.flush()
                    logging.info(f"Produced weather data for {api_name} - {city}")
                except Exception as e:
                    logging.error(f"Failed to produce data for {api_name} - {city} | Error: {e}")


if __name__ == '__main__':
    while True:
        try:
            push_weather_data_to_kafka()
            time.sleep(15 * 60)
        except Exception as e:
            logging.critical(f"Unexpected error in main loop: {e}", exc_info=True)
            time.sleep(60)
