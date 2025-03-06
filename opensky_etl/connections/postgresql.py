"""
PostgreSQL connection module for the OpenSky ETL pipeline.
"""
import os
from sqlalchemy import create_engine, inspect, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from config.settings import (
    DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, SQLITE_PATH
)
from utils.logging_config import get_logger

# Initialize logger
logger = get_logger("connections.postgresql")

# Create base class for declarative models
Base = declarative_base()

class FlightData(Base):
    """SQLAlchemy model for flight data."""
    __tablename__ = 'flight_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    icao24 = Column(String(24))
    firstSeen = Column(Integer, index=True)
    estDepartureAirport = Column(String(4))
    lastSeen = Column(Integer, index=True)
    estArrivalAirport = Column(String(4))
    callsign = Column(String(8))
    estDepartureAirportHorizDistance = Column(Integer)
    estDepartureAirportVertDistance = Column(Integer)
    estArrivalAirportHorizDistance = Column(Integer)
    estArrivalAirportVertDistance = Column(Integer)
    departureAirportCandidatesCount = Column(Integer)
    arrivalAirportCandidatesCount = Column(Integer)
    
    # Additional columns for transformations
    flight_duration_minutes = Column(Float)
    total_distance_km = Column(Float)
    airport_pair = Column(String(10))
    
    def __repr__(self):
        return f"<Flight(icao24='{self.icao24}', callsign='{self.callsign}')>"

def get_db_connection():
    """
    Create database connection with fallback to SQLite if PostgreSQL fails.
    
    Returns:
        tuple: (engine, session, metadata) for database operations
    """
    try:
        # Try to create PostgreSQL engine
        logger.info("Attempting to connect to PostgreSQL...")
        connection_string = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)
        engine.connect()  # Test connection
        logger.info(f"Successfully connected to {DB_NAME} at {DB_HOST}")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        
        # Fall back to SQLite as a last resort
        logger.warning("Falling back to SQLite database...")
        engine = create_engine(f"sqlite:///{SQLITE_PATH}")
        logger.info(f"Using SQLite database at {SQLITE_PATH}")
    
    # Create all tables if they don't exist
    Base.metadata.create_all(engine)
    
    # Create a session factory
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Create metadata object
    metadata = MetaData()
    metadata.reflect(bind=engine)
    
    return engine, session, metadata

def get_last_incremental_value(engine, table_name, column_name):
    """
    Get the last value of the incremental column for incremental loading.
    
    Args:
        engine: SQLAlchemy engine
        table_name (str): Table name
        column_name (str): Column name for incremental loading
        
    Returns:
        int: Last value of the incremental column or 0 if not found
    """
    try:
        inspector = inspect(engine)
        if table_name in inspector.get_table_names():
            with engine.connect() as connection:
                # Use double quotes around column name to preserve case
                query = text(f'SELECT MAX("{column_name}") FROM {table_name}')
                result = connection.execute(query).scalar()
                return result if result is not None else 0
        return 0
    except Exception as e:
        logger.error(f"Error getting last incremental value: {e}")
        return 0
    
    
def execute_sql_from_file(engine, sql_file_path, params=None):
    """
    Execute SQL from a file.
    
    Args:
        engine: SQLAlchemy engine
        sql_file_path (str): Path to SQL file
        params (dict): Parameters to substitute in the SQL
        
    Returns:
        list: Result of the SQL execution
    """
    try:
        # Correct path to look in the package directory
        script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        full_path = os.path.join(script_dir, sql_file_path)
        
        logger.debug(f"Looking for SQL file at: {full_path}")
        
        # Check if file exists
        if not os.path.exists(full_path):
            logger.error(f"SQL file not found: {full_path}")
            return []
            
        with open(full_path, 'r') as file:
            sql = file.read()
            
        if params:
            # Replace parameters in SQL
            for key, value in params.items():
                sql = sql.replace(f"{{{{{key}}}}}", str(value))
        
        # Check if we're using SQLite
        is_sqlite = 'sqlite' in str(engine.url).lower()
        
        with engine.connect() as connection:
            if is_sqlite:
                # For SQLite, execute with commit
                result = connection.execute(text(sql))
                connection.commit()
            else:
                # For PostgreSQL
                result = connection.execute(text(sql))
                
            return result.fetchall()
    except Exception as e:
        logger.error(f"Error executing SQL from file {sql_file_path}: {e}")
        return []