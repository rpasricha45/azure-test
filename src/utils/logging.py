import logging
import sys
from pathlib import Path
from src.config.settings import FileConfig, ENV

def setup_logging(name: str = None) -> logging.Logger:
    """Configure logging with proper formatting and handling"""
    logger = logging.getLogger(name or __name__)
    
    if not logger.handlers:
        logger.setLevel(logging.DEBUG if ENV == 'development' else logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(console_handler)
        
        # File handler
        log_file = FileConfig.OUTPUT_DIR / 'app.log'
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        logger.addHandler(file_handler)
    
    return logger 