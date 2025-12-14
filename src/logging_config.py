# src/logging_config.py
import logging
from logging.handlers import RotatingFileHandler
import os
import sys

def setup_logging():

    # Don't reconfigure if running in pytest
    #if os.getenv('PYTEST_CURRENT_TEST'):
    #    return  # Skip - let pytest handle logging
    
    log_level = os.getenv('LOG_LEVEL', 'INFO')  # defaults to INFO when not set
    scraper_type = os.getenv('SCRAPER_TYPE', 'unknown')
    scraper_type_upper = scraper_type.upper()
    
    # Create logs directory
    os.makedirs('./data/logs', exist_ok=True)
    
    # Individual file for this scraper (no prefix needed)
    individual_log = f'./data/logs/scraper_{scraper_type}.log'
    individual_handler = RotatingFileHandler(
        individual_log,
        maxBytes=50*1024*1024,  # 50MB
        backupCount=2
    )
    individual_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    
    # Combined file for all scrapers (with prefix to identify source)
    combined_log = './data/logs/scraper_all.log'
    combined_handler = RotatingFileHandler(
        combined_log,
        maxBytes=100*1024*1024,  # 100MB
        backupCount=5
    )
    combined_handler.setFormatter(logging.Formatter(
        f'%(asctime)s - [{scraper_type_upper}] - %(name)s - %(levelname)s - %(message)s'
    ))
    
    # Console output with prefix
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(
        f'%(asctime)s - [{scraper_type_upper}] - %(name)s - %(levelname)s - %(message)s'
    ))
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        handlers=[console_handler, individual_handler, combined_handler],
        force=True
    )
    
    logger = logging.getLogger(__name__)
    logger.info("="*60)
    logger.info(f"Logging initialized for scraper type: {scraper_type}")
    logger.info(f"Individual log: {individual_log}")
    logger.info(f"Combined log: {combined_log}")
    logger.info(f"Log level: {log_level}")
    logger.info("="*60)