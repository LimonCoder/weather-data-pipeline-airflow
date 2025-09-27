import os
import psycopg2
from sqlalchemy import create_engine
from urllib.parse import quote_plus

from dotenv import load_dotenv
load_dotenv()

# PostgreSQL configuration
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST',)
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT'))
POSTGRES_DB = os.getenv('POSTGRES_DB')


def get_connection():
    """Get a new database connection"""
    try:
        return psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port=POSTGRES_PORT
        )
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        raise


# SQLAlchemy engine for ORM operations
# URL-encode the password to handle special characters
encoded_password = quote_plus(POSTGRES_PASSWORD)
engine = create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{encoded_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')

# For backward compatibility, create a connection instance (use with caution)
try:
    connection = get_connection()
except Exception as e:
    print(f"Warning: Could not establish initial connection: {e}")
    connection = None


