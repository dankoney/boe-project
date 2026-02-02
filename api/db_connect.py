import psycopg2
from fastapi import HTTPException
import sys

# --- 1. Database Connection Details (AWS) ---
DB_NAME = "postgres" 
DB_USER = "postgres"
DB_PASS = "Danlyn2021" 
DB_HOST = "declaration-db.ct8kgqkmy6bu.eu-north-1.rds.amazonaws.com"
DB_PORT = "5432"


# --- 1. Database Connection Details (Localhost) ---
#DB_NAME = "Declaration_db" 
#DB_USER = "postgres"
#DB_PASS = "Danlyn@2021" 
#DB_HOST = "localhost"
#DB_PORT = "5432"


def get_api_connection():
    """
    Establishes a new PostgreSQL connection for the API server.
    Raises HTTPException on failure (for the web client).
    """
    try:
        conn = psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Database connection failed: {e}")
        # Use 503 Service Unavailable for API responses
        raise HTTPException(status_code=503, detail="Database service unavailable.")

def get_loader_connection():
    """
    Establishes a new PostgreSQL connection for the batch loader script.
    Sets autocommit=False for transactions and exits on failure.
    """
    try:
        conn = psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = False # Essential for the batch loading script
        return conn
    except psycopg2.OperationalError as e:
        print(f"\n--- DATABASE CONNECTION ERROR ---")
        print("Please check credentials and ensure PostgreSQL is running.")
        print(f"Error: {e}")
        sys.exit(1)