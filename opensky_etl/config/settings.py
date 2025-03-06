
"""
Configuration settings for the OpenSky ETL pipeline.
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(".env")

# OpenSky API credentials
OPENSKY_USERNAME = os.getenv("OPENSKY_USERNAME")
OPENSKY_PASSWORD = os.getenv("OPENSKY_PASSWORD")

# Database configuration
DB_USERNAME = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "db-raw-opensky.cjeesaow22lr.eu-central-1.rds.amazonaws.com")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "db-raw-opensky")

# SQLite fallback config
SQLITE_PATH = "flight_data.db"

# Time window for extraction (in seconds)
EXTRACTION_WINDOW = 86400  # 24 hours
API_INTERVAL = 7200  # 2 hours for API request chunking

# Logging configuration
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = "etl_logs.log"
LOG_LEVEL = "INFO"

# Incremental load configuration
INCREMENTAL_COLUMN = "lastSeen"
INCREMENTAL_TABLE = "flight_data"