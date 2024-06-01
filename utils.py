import psycopg2
from psycopg2.extras import execute_values
import os

def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "zipcodemx"),
        user=os.getenv("DB_USER", "thor"),
        password=os.getenv("DB_PASS", "34198yVmaPnc;&*5976")
    )
    return conn