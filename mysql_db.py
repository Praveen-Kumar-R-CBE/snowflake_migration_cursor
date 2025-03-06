import mysql.connector
from mysql.connector import Error
import pandas as pd
from sqlalchemy import create_engine
from utils.logger import DatabaseLogger

class MySQLConnection:
    def __init__(self, host="localhost", user="user", 
                 password="password", database="db"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.engine = None
        self.logger = DatabaseLogger()
        
    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            # Create SQLAlchemy engine
            self.engine = create_engine(
                f'mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.database}'
            )
            if self.connection.is_connected():
                self.logger.log_info("MySQL", f"Connected to database: {self.database}")
                return True
        except Error as e:
            self.logger.log_error("MySQL", str(e))
            return False
            
    def get_tables(self):
        try:
            if self.connection and self.connection.is_connected():
                cursor = self.connection.cursor()
                query = "SHOW TABLES"
                self.logger.log_query("MySQL", query)
                cursor.execute(query)
                tables = cursor.fetchall()
                return [table[0] for table in tables]
        except Error as e:
            self.logger.log_error("MySQL", f"Error fetching tables: {str(e)}")
            return []
            
    def query_table(self, table_name):
        try:
            if self.engine:
                query = f"SELECT * FROM {table_name}"
                self.logger.log_query("MySQL", query)
                return pd.read_sql(query, self.engine)
        except Error as e:
            self.logger.log_error("MySQL", f"Error querying table {table_name}: {str(e)}")
            return None
            
    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.log_info("MySQL", "Connection closed") 