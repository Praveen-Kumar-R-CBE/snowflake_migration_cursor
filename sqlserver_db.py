import pyodbc
import pandas as pd
from utils.logger import DatabaseLogger
from sqlalchemy import create_engine
import urllib.parse

class SQLServerConnection:
    def __init__(self, server, database, username=None, password=None, 
                 trusted_connection=False, driver=None, port=1433):
        # Initialize logger first
        self.logger = DatabaseLogger()
        
        self.server = server
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.trusted_connection = trusted_connection
        self.connection = None
        self.engine = None
        
        # Get driver after logger is initialized
        self.driver = self._get_available_driver(driver)
        
    def _get_available_driver(self, preferred_driver=None):
        """Get an available SQL Server driver"""
        # Log available drivers
        available_drivers = pyodbc.drivers()
        self.logger.log_info("SQL Server", f"Available ODBC drivers: {available_drivers}")
        
        # Return specific driver instead of checking available ones
        driver = "ODBC Driver 18 for SQL Server"
        self.logger.log_info("SQL Server", f"Using driver: {driver}")
        return driver
        
    def connect(self):
        try:
            # Log connection attempt
            self.logger.log_info("SQL Server", f"Attempting to connect to {self.server}:{self.port}/{self.database}")
            
            # Format server with port
            server_with_port = f"{self.server},{self.port}"
            
            conn_str = (
                f'DRIVER={{{self.driver}}};'
                f'SERVER={server_with_port};'
                f'DATABASE={self.database};'
                f'UID={self.username};'
                f'PWD={self.password};'
                'Trusted_Connection=no;'  # or 'yes' if using Windows Authentication
                'TrustServerCertificate=yes;'
            )
            
            # Log connection string (without password)
            safe_conn_str = conn_str.replace(self.password, '****') if self.password else conn_str
            self.logger.log_info("SQL Server", f"Connection string: {safe_conn_str}")
            
            self.connection = pyodbc.connect(conn_str)
            
            # Create SQLAlchemy engine
            params = urllib.parse.quote_plus(conn_str)
            engine_str = f'mssql+pyodbc:///?odbc_connect={params}'
            
            self.engine = create_engine(engine_str)
            
            self.logger.log_info("SQL Server", f"Connected to database: {self.database}")
            return True
        except Exception as e:
            self.logger.log_error("SQL Server", str(e))
            return False
            
    def get_tables(self):
        try:
            if self.connection:
                cursor = self.connection.cursor()
                query = """
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
                """
                self.logger.log_query("SQL Server", query)
                cursor.execute(query)
                tables = cursor.fetchall()
                return [table[0] for table in tables]
        except Exception as e:
            self.logger.log_error("SQL Server", f"Error fetching tables: {str(e)}")
            return []
            
    def query_table(self, table_name):
        try:
            if self.engine:
                query = f"SELECT * FROM {table_name}"
                self.logger.log_query("SQL Server", query)
                return pd.read_sql(query, self.engine)
        except Exception as e:
            self.logger.log_error("SQL Server", f"Error querying table {table_name}: {str(e)}")
            return None
            
    def close(self):
        if self.connection:
            self.connection.close()
            self.logger.log_info("SQL Server", "Connection closed") 