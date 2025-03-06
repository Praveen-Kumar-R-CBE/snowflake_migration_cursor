import logging
from datetime import datetime
import os

class DatabaseLogger:
    def __init__(self):
        # Create logs directory if it doesn't exist
        if not os.path.exists('logs'):
            os.makedirs('logs')
            
        # Create logger
        self.logger = logging.getLogger('database_operations')
        
        # Only add handler if logger doesn't have handlers
        if not self.logger.handlers:
            self.logger.setLevel(logging.INFO)
            
            # Create file handler with daily log file
            log_filename = f"logs/db_operations_{datetime.now().strftime('%Y%m%d')}.log"
            file_handler = logging.FileHandler(log_filename)
            file_handler.setLevel(logging.INFO)
            
            # Create formatter
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            
            # Add handler to logger
            self.logger.addHandler(file_handler)
    
    def log_query(self, database_type, query, status="executed"):
        self.logger.info(f"[{database_type}] Query {status}: {query}")
    
    def log_error(self, database_type, error_message):
        self.logger.error(f"[{database_type}] Error: {error_message}")
    
    def log_info(self, database_type, message):
        self.logger.info(f"[{database_type}] {message}")
    
    def log_warning(self, database_type, message):
        self.logger.warning(f"[{database_type}] Warning: {message}") 