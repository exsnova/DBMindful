# models/database.py
import psycopg2
from psycopg2.extras import DictCursor
from config.settings import settings
import contextlib

class DatabaseConnection:
    @staticmethod
    @contextlib.contextmanager
    def get_connection():
        conn = None
        try:
            conn = psycopg2.connect(**settings.db_params)
            yield conn
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def test_connection():
        try:
            with DatabaseConnection.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT version();')
                    version = cur.fetchone()
                    return f"Successfully connected to PostgreSQL. Version: {version[0]}"
        except Exception as e:
            return f"Connection failed: {str(e)}"