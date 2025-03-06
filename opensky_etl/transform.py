"""
Transform module for the OpenSky ETL pipeline.
"""
import os
import pandas as pd
import re
from jinja2 import Template
from sqlalchemy import text

from utils.logging_config import get_logger
from connections.postgresql import execute_sql_from_file

# Initialize logger
logger = get_logger("transform")

def apply_sql_transformation(engine, sql_path, is_incremental=False, last_value=0):
    """
    Apply SQL transformation from a file using Jinja2-style templating.
    
    Args:
        engine: SQLAlchemy engine
        sql_path (str): Path to SQL file
        is_incremental (bool): Whether to use incremental loading
        last_value (int): Last incremental value
        
    Returns:
        pd.DataFrame: Transformed data
    """
    logger.info(f"Applying SQL transformation from {sql_path}")
    
    try:
        # Correct path to look in the package directory
        script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        full_path = os.path.join(script_dir, sql_path)
        
        logger.debug(f"Looking for SQL file at: {full_path}")
        
        # Check if file exists
        if not os.path.exists(full_path):
            logger.error(f"SQL file not found: {full_path}")
            return pd.DataFrame()
        
        # Read SQL file
        with open(full_path, 'r') as f:
            sql_template = f.read()
        
        # Replace Jinja2-style templates
        sql = sql_template.replace("{% if is_incremental %}", "" if is_incremental else "/*")
        sql = sql.replace("{% endif %}", "" if is_incremental else "*/")
        sql = sql.replace("{{last_incremental_value}}", str(last_value))
        
        # PostgreSQL needs quoted column names to preserve case sensitivity
        # This is a simple replacement - for complex SQL, consider a real SQL parser
        is_postgresql = 'postgresql' in str(engine.url)
        if is_postgresql:
            # Simple regex to find column references
            column_pattern = r'([a-zA-Z][a-zA-Z0-9_]*\.[a-zA-Z][a-zA-Z0-9_]*)'
            
            def add_quotes(match):
                parts = match.group(1).split('.')
                return f'"{parts[0]}"."{parts[1]}"'
                
            sql = re.sub(column_pattern, add_quotes, sql)
        
        logger.debug(f"Processed SQL: {sql[:500]}...")  # Log first 500 chars of SQL
        
        # Execute query
        with engine.connect() as conn:
            result = pd.read_sql(text(sql), conn)
            
        logger.info(f"Transformation complete. Returned {len(result)} rows.")
        return result
    
    except Exception as e:
        logger.error(f"Error applying SQL transformation: {e}")
        return pd.DataFrame()

def transform_flight_data(df, engine, is_incremental=False, last_value=0):
    """
    Apply transformations to flight data.
    
    Args:
        df (pd.DataFrame): Flight data DataFrame
        engine: SQLAlchemy engine
        is_incremental (bool): Whether to use incremental loading
        last_value (int): Last incremental value
        
    Returns:
        pd.DataFrame: Transformed flight data
    """
    logger.info(f"Starting flight data transformation with {len(df)} records")
    
    if df.empty:
        logger.warning("Empty DataFrame, skipping transformations")
        return df
    
    try:
        # Since SQL transformations might not work initially, let's add direct transformations
        df = calculate_flight_duration(df)
        df = calculate_flight_distance(df)
        df = create_airport_pairs(df)
        
        logger.info("Transformations applied successfully")
        return df
        
    except Exception as e:
        logger.error(f"Error in transformations: {e}")
        return df

def calculate_flight_duration(df):
    """
    Calculate flight duration in minutes.
    
    Args:
        df (pd.DataFrame): Flight data DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with flight duration
    """
    logger.info("Calculating flight duration")
    
    try:
        df['flight_duration_minutes'] = (df['lastSeen'] - df['firstSeen']) / 60.0
        return df
    except Exception as e:
        logger.error(f"Error calculating flight duration: {e}")
        return df

def calculate_flight_distance(df):
    """
    Calculate approximate flight distance in kilometers.
    
    Args:
        df (pd.DataFrame): Flight data DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with flight distance
    """
    logger.info("Calculating flight distance")
    
    try:
        # This is a simplified approximation
        df['total_distance_km'] = (
            df['estDepartureAirportHorizDistance'].fillna(0) + 
            df['estArrivalAirportHorizDistance'].fillna(0)
        ) / 1000.0
        return df
    except Exception as e:
        logger.error(f"Error calculating flight distance: {e}")
        return df

def create_airport_pairs(df):
    """
    Create airport pair identifiers.
    
    Args:
        df (pd.DataFrame): Flight data DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with airport pairs
    """
    logger.info("Creating airport pairs")
    
    try:
        df['airport_pair'] = df.apply(
            lambda row: f"{row['estDepartureAirport']}-{row['estArrivalAirport']}" 
            if pd.notna(row['estDepartureAirport']) and pd.notna(row['estArrivalAirport']) 
            else None, 
            axis=1
        )
        return df
    except Exception as e:
        logger.error(f"Error creating airport pairs: {e}")
        return df