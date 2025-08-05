"""
This file provides a function to Trino and PostgreSQL databases using Ibis.
It handles credential management, connection setup, and error handling.
"""
import marimo as mo
import os
import io
import ibis
from dotenv import load_dotenv
import trino.auth
# import ibis.backends.base # Import for type hinting

_ = load_dotenv()

def connect_to_database(db_type: str, schema: str = None, enable_logging: bool = False):
    """
    General function to connect to either Trino or PostgreSQL based on `db_type`.

    Args:
        db_type (str): The type of database to connect to. Accepts 'trino' or 'postgres'.
        schema (str, optional): The schema to connect to. Defaults to None for PostgreSQL, 'iceberg-ingest' for Trino.
        enable_logging (bool, optional): If True, enables verbose logging. Defaults to False.

    Returns:
        ibis.backends.base.BaseBackend: An Ibis connection object.
    """
    if enable_logging:
        query_logs = io.StringIO()
        ibis.options.verbose_log = lambda msg: print(msg, file=query_logs)
        ibis.options.verbose = True

    try:
        if db_type == 'trino':
            # Trino connection parameters
            host = os.environ.get("TRINO_HOST")
            user = os.environ.get("TRINO_USER")
            database = "iceberg"
            schema = schema or "iceberg-ingest"
            port = os.environ.get("TRINO_PORT", "443")
            
            # Check credentials
            if not all([host, user]):
                raise ValueError("Missing Trino credentials.")
            
            trino_conn = ibis.trino.connect(
                host=host,
                port=port,
                user=user,
                database=database,
                schema=schema,
                auth = trino.auth.OAuth2Authentication(),
                http_scheme="https",  # Handles the weird header stripping error
            )
            return trino_conn

        elif db_type == 'postgres':
            # PostgreSQL connection parameters
            host = os.getenv("ECDN11_PGHOST")
            user = os.getenv("ECDN11_PGUSERNAME")
            password = os.getenv("ECDN11_PGPASSWORD")
            database = os.getenv("ECDN11_PGDATABASE")
            port = os.getenv("ECDN11_PGPORT", "5432")

            # Check credentials
            if not all([host, user, database]):
                raise ValueError("Missing PostgreSQL credentials.")
            
            pg_conn = ibis.connect(f"postgresql://{user}:{password}@{host}:{port}/{database}")
            mo.md("### 🎉 **Successfully connected to PostgreSQL!**")
            return pg_conn

        else:
            raise ValueError("Invalid database type. Choose 'trino' or 'postgres'.")

    except Exception as e:
        mo.stop(
            True,
            mo.md(f"❌ Failed to connect to {db_type.capitalize()}: {e}. Please check your credentials and network connectivity.")
        )

# Test

if __name__ == "__main__":
    # Example usage
    try:
        trino_conn = connect_to_database('trino', enable_logging=True)
        print("Connected to Trino successfully!")
    except Exception as e:
        print(f"Error connecting to Trino: {e}")