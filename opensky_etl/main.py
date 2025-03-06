#!/usr/bin/env python
"""
Main entry point for the OpenSky ETL pipeline.
"""
import argparse
import os
import sys
from datetime import datetime

# Add parent directory to path to import modules if necessary
# Uncomment this if main.py is not in the same directory as the opensky_etl package
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import docker_init
except ImportError:
    # Not in Docker, continue without it
    pass

from utils.logging_config import setup_logging
from pipelines.flight_data_pipeline import FlightDataPipeline

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='OpenSky ETL Pipeline')
    parser.add_argument('--full', action='store_true', help='Force full load instead of incremental')
    parser.add_argument('--log-level', default='INFO', help='Logging level')
    return parser.parse_args()

def main():
    """Main entry point."""
    # Parse command line arguments
    args = parse_args()
    
    # Setup logging
    logger = setup_logging(log_level=args.log_level)
    logger.info("Starting OpenSky ETL pipeline")
    
    # Print timestamp
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Current time: {current_time}")
    
    # Initialize and run pipeline
    pipeline = FlightDataPipeline()
    success = pipeline.run(force_full_load=args.full)
    
    # Print pipeline statistics
    stats = pipeline.get_stats()
    logger.info(f"Pipeline statistics: {stats}")
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()