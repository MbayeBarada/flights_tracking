"""
Flight data ETL pipeline.
"""
import os
import time
from datetime import datetime, timezone

from config.settings import (
    INCREMENTAL_COLUMN, INCREMENTAL_TABLE
)
from connections.postgresql import (
    get_db_connection, get_last_incremental_value
)
from extract import extract_flight_data, extract_incremental_data
from transform import transform_flight_data
from load import load_data_to_db, create_summary_views
from utils.logging_config import setup_logging, get_logger

class FlightDataPipeline:
    """Flight data ETL pipeline."""
    
    def __init__(self):
        """Initialize the pipeline."""
        self.logger = get_logger("pipelines.flight_data")
        self.logger.info("Initializing flight data pipeline")
        
        # Get database connection
        self.engine, self.session, self.metadata = get_db_connection()
        
        # Track pipeline execution
        self.start_time = None
        self.end_time = None
        self.records_processed = 0
        self.is_incremental = False
        self.last_value = 0
    
    def run(self, force_full_load=False):
        """
        Run the ETL pipeline.
        
        Args:
            force_full_load (bool): Force a full load instead of incremental
            
        Returns:
            bool: Success status
        """
        self.start_time = time.time()
        self.logger.info("Starting flight data ETL pipeline")
        
        try:
            # Determine if we should do incremental or full load
            self.is_incremental = not force_full_load
            
            if self.is_incremental:
                # Get last timestamp for incremental loading
                self.last_value = get_last_incremental_value(
                    self.engine, INCREMENTAL_TABLE, INCREMENTAL_COLUMN
                )
                
                if self.last_value:
                    self.logger.info(f"Running incremental load from timestamp {self.last_value}")
                    last_date = datetime.fromtimestamp(self.last_value, timezone.utc)
                    self.logger.info(f"Last processed date: {last_date}")
                    
                    # Extract new data
                    df = extract_incremental_data(self.last_value)
                else:
                    self.logger.info("No previous data found, running full load")
                    self.is_incremental = False
                    df = extract_flight_data()
            else:
                self.logger.info("Running full load")
                df = extract_flight_data()
            
            if df.empty:
                self.logger.warning("No data extracted, ending pipeline")
                return False
            
            # Transform data
            self.logger.info("Transforming data")
            df_transformed = transform_flight_data(
                df, self.engine, self.is_incremental, self.last_value
            )
            
            # Load data
            self.logger.info("Loading data to database")
            self.records_processed = load_data_to_db(
                df_transformed, self.engine, self.session, self.is_incremental
            )
            
            # Create summary views if we processed any records
            if self.records_processed > 0:
                self.logger.info("Creating summary views")
                create_summary_views(self.engine)
            
            self.end_time = time.time()
            duration = self.end_time - self.start_time
            self.logger.info(f"Pipeline completed in {duration:.2f} seconds")
            self.logger.info(f"Processed {self.records_processed} records")
            
            return True
        
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            return False
        finally:
            self.session.close()
    
    def get_stats(self):
        """
        Get pipeline execution statistics.
        
        Returns:
            dict: Pipeline statistics
        """
        return {
            "start_time": datetime.fromtimestamp(self.start_time) if self.start_time else None,
            "end_time": datetime.fromtimestamp(self.end_time) if self.end_time else None,
            "duration_seconds": round(self.end_time - self.start_time, 2) if self.end_time and self.start_time else None,
            "records_processed": self.records_processed,
            "is_incremental": self.is_incremental,
            "last_incremental_value": self.last_value
        }