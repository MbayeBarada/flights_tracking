"""
Load module for the OpenSky ETL pipeline.
"""
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from connections.postgresql import FlightData
from utils.logging_config import get_logger

# Initialize logger
logger = get_logger("load")

def load_data_to_db(df, engine, session, is_incremental=True):
    """
    Load flight data into the database.
    
    Args:
        df (pd.DataFrame): Flight data DataFrame
        engine: SQLAlchemy engine
        session: SQLAlchemy session
        is_incremental (bool): Whether to use incremental loading
        
    Returns:
        int: Number of records loaded
    """
    logger.info(f"Starting load operation for {len(df)} records")
    
    if df.empty:
        logger.warning("Empty DataFrame, skipping load operation")
        return 0
    
    try:
        # Handle potential missing columns in the DataFrame
        required_columns = [
            'icao24', 'firstSeen', 'estDepartureAirport', 'lastSeen', 
            'estArrivalAirport', 'callsign', 'estDepartureAirportHorizDistance',
            'estDepartureAirportVertDistance', 'estArrivalAirportHorizDistance', 
            'estArrivalAirportVertDistance', 'departureAirportCandidatesCount',
            'arrivalAirportCandidatesCount'
        ]
        
        # Add transformation columns if they exist
        optional_columns = [
            'flight_duration_minutes', 'total_distance_km', 'airport_pair'
        ]
        
        # Create empty columns if they don't exist
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"Missing column {col}, creating empty column")
                df[col] = None
        
        # Add optional transformation columns if they don't exist
        for col in optional_columns:
            if col not in df.columns:
                logger.warning(f"Missing transformation column {col}, creating empty column")
                df[col] = None
        
        # Bulk insert approach for better performance
        flight_records = []
        for _, row in df.iterrows():
            # Convert row to dictionary, handling any missing columns
            flight_data = {}
            
            # Add required columns
            for col in required_columns:
                flight_data[col] = row.get(col) if col in row else None
            
            # Add optional transformation columns
            for col in optional_columns:
                if col in row and not pd.isna(row[col]):
                    flight_data[col] = row[col]
            
            # Create FlightData object
            flight_records.append(FlightData(**flight_data))
        
        # Add all records to the session
        session.bulk_save_objects(flight_records)
        
        # Commit the transaction
        session.commit()
        
        logger.info(f"Successfully loaded {len(flight_records)} records into the flight_data table.")
        return len(flight_records)
    
    except Exception as e:
        session.rollback()
        logger.error(f"Error loading data into database: {e}")
        logger.error(f"Exception details: {str(e)}")
        return 0

def create_or_replace_view(engine, view_name, sql):
    """
    Create or replace a database view.
    
    Args:
        engine: SQLAlchemy engine
        view_name (str): Name of the view
        sql (str): SQL query for the view
        
    Returns:
        bool: Success status
    """
    logger.info(f"Creating or replacing view {view_name}")
    
    try:
        with engine.connect() as conn:
            # Check if we're using SQLite
            is_sqlite = 'sqlite' in str(engine.url).lower()
            
            # Drop view if exists
            if is_sqlite:
                # SQLite specific approach
                conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
                conn.commit()
            else:
                # PostgreSQL approach
                conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
                
            # Create view
            create_view_sql = f"CREATE VIEW {view_name} AS {sql}"
            
            if is_sqlite:
                conn.execute(text(create_view_sql))
                conn.commit()
            else:
                conn.execute(text(create_view_sql))
            
            logger.info(f"Successfully created view {view_name}")
            return True
    
    except Exception as e:
        logger.error(f"Error creating view {view_name}: {e}")
        return False

def create_summary_views(engine):
    """
    Create summary views for flight data analysis.
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        bool: Success status
    """
    logger.info("Creating summary views")
    
    try:
        # Create airport activity view
        airport_view_sql = """
        SELECT 
            "estDepartureAirport" AS airport_code,
            COUNT(*) AS departure_count,
            MIN("firstSeen") AS first_activity,
            MAX("lastSeen") AS last_activity
        FROM 
            flight_data
        WHERE 
            "estDepartureAirport" IS NOT NULL
        GROUP BY 
            "estDepartureAirport"
        """
        airport_result = create_or_replace_view(engine, "airport_departures", airport_view_sql)
        
        # Create flight duration view
        duration_view_sql = """
        SELECT 
            "icao24",
            "callsign",
            "estDepartureAirport",
            "estArrivalAirport",
            "airport_pair",
            "firstSeen",
            "lastSeen",
            "flight_duration_minutes",
            "total_distance_km"
        FROM 
            flight_data
        WHERE 
            "flight_duration_minutes" IS NOT NULL
        ORDER BY 
            "flight_duration_minutes" DESC
        """
        duration_result = create_or_replace_view(engine, "flight_durations", duration_view_sql)
        
        logger.info("Successfully created summary views")
        return airport_result and duration_result
    
    except Exception as e:
        logger.error(f"Error creating summary views: {e}")
        return False