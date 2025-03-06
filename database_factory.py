from mysql_db import MySQLConnection
from sqlserver_db import SQLServerConnection
from snowflake_db import SnowflakeConnection

class DatabaseFactory:
    @staticmethod
    def create_connection(db_type, **config):
        db_type = db_type.lower().replace(" ", "")  # Convert to lowercase and remove spaces
        
        if db_type == 'mysql':
            return MySQLConnection(**config)
        elif db_type == 'sqlserver':
            return SQLServerConnection(**config)
        elif db_type == 'snowflake':
            return SnowflakeConnection(**config)
        else:
            raise ValueError(f"Unsupported database type: {db_type}") 