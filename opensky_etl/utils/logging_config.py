"""
Logging configuration for the OpenSky ETL pipeline.
"""
import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime
from config.settings import LOG_FORMAT, LOG_FILE, LOG_LEVEL

def setup_logging(name="opensky_etl", log_level=None):
    """
    Configure logging for the ETL pipeline.
    
    Args:
        name (str): Logger name
        log_level (str, optional): Logging level (e.g., "INFO", "DEBUG"). 
                                  Defaults to value from settings.
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Generate log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"logs/{LOG_FILE.replace('.log', '')}_{timestamp}.log"
    
    # Use provided log_level or default from settings
    level = log_level if log_level else LOG_LEVEL
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level))
    
    # Remove existing handlers if any
    if logger.handlers:
        logger.handlers.clear()
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level))
    console_format = logging.Formatter(LOG_FORMAT)
    console_handler.setFormatter(console_format)
    
    # Create file handler
    file_handler = RotatingFileHandler(
        log_filename, 
        maxBytes=10485760,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(getattr(logging, level))
    file_format = logging.Formatter(LOG_FORMAT)
    file_handler.setFormatter(file_format)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def get_logger(module_name):
    """
    Get a logger for a specific module.
    
    Args:
        module_name (str): Module name
        
    Returns:
        logging.Logger: Logger for the module
    """
    return logging.getLogger(f"opensky_etl.{module_name}")