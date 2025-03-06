"""
Extract module for the OpenSky ETL pipeline.
"""
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta

from config.settings import (
    OPENSKY_USERNAME, OPENSKY_PASSWORD, 
    EXTRACTION_WINDOW, API_INTERVAL
)
from utils.logging_config import get_logger

# Initialize logger
logger = get_logger("extract")

def extract_flight_data(start_time=None, end_time=None):
    """
    Extract flight data from OpenSky API.
    
    Args:
        start_time (int, optional): Start timestamp. Defaults to 24 hours ago.
        end_time (int, optional): End timestamp. Defaults to current time.
        
    Returns:
        pd.DataFrame: DataFrame with flight data
    """
    logger.info("Starting flight data extraction from OpenSky API")
    
    # Check credentials
    if not OPENSKY_USERNAME or not OPENSKY_PASSWORD:
        error_msg = "Missing OpenSky credentials. Check your .env file."
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Define time range if not provided
    if end_time is None:
        end_time = int(datetime.now(timezone.utc).timestamp())  # Current UTC time
    
    if start_time is None:
        start_time = end_time - EXTRACTION_WINDOW  # Default to 24 hours ago
    
    logger.info(f"Extracting flight data from {datetime.fromtimestamp(start_time, timezone.utc)} "
                f"to {datetime.fromtimestamp(end_time, timezone.utc)}")
    
    # Initialize empty list to store data
    all_flights = []
    
    # Loop through intervals (to comply with OpenSky limits)
    interval = API_INTERVAL  # 2 hours in seconds
    current_start = start_time
    while current_start < end_time:
        current_end = min(current_start + interval, end_time)
        
        # OpenSky API URL for current window
        url = f"https://opensky-network.org/api/flights/all?begin={current_start}&end={current_end}"
        
        logger.debug(f"Fetching data from {url}")
        
        try:
            # Fetch data from OpenSky API
            response = requests.get(url, auth=(OPENSKY_USERNAME, OPENSKY_PASSWORD))
            
            # Check for successful response
            if response.status_code == 200:
                flights = response.json()
                logger.debug(f"Retrieved {len(flights)} flights for time window")
                all_flights.extend(flights)
            else:
                logger.error(f"Error {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"Exception during API request: {e}")
        
        # Move to next window
        current_start = current_end
    
    # Convert collected data to DataFrame
    df_flights = pd.DataFrame(all_flights)
    
    logger.info(f"Extraction complete. Retrieved {len(df_flights)} flight records.")
    
    return df_flights

def extract_incremental_data(last_value):
    """
    Extract incremental flight data from OpenSky API.
    
    Args:
        last_value (int): Last timestamp value for incremental loading
        
    Returns:
        pd.DataFrame: DataFrame with new flight data
    """
    logger.info(f"Starting incremental extraction from timestamp {last_value}")
    
    # Convert timestamp to datetime for logging
    last_datetime = datetime.fromtimestamp(last_value, timezone.utc) if last_value else None
    logger.info(f"Last processed timestamp: {last_datetime}")
    
    # Extract data from last timestamp to now
    return extract_flight_data(start_time=last_value)