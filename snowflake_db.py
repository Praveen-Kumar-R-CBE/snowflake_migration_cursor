import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from utils.logger import DatabaseLogger
import os
import tempfile
from datetime import datetime
import yaml  # Add this import at the top with other imports

class SnowflakeConnection:
    def __init__(self, account, user, password, warehouse, database, schema):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database.upper()
        self.schema = schema.upper()
        self.connection = None
        self.logger = DatabaseLogger()
        self.stage_name = 'MYSQL_MIGRATION_STAGE'
        self.chunk_size = 100000  # Number of rows per file
        self.data_dir = 'data_files'  # Directory to store split files
        self.type_mapping = self._load_type_mapping()
        
    def connect(self):
        try:
            self.connection = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            # Create internal stage if it doesn't exist
            self._create_stage()
            self._create_data_directory()
            self.logger.log_info("Snowflake", f"Connected to database: {self.database}")
            return True
        except Exception as e:
            self.logger.log_error("Snowflake", str(e))
            return False
    
    def _create_data_directory(self):
        """Create directory for storing split files if it doesn't exist"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            
    def _create_stage(self):
        try:
            cursor = self.connection.cursor()
            create_stage_sql = f"CREATE STAGE IF NOT EXISTS {self.stage_name}"
            self.logger.log_query("Snowflake", create_stage_sql)
            cursor.execute(create_stage_sql)
        except Exception as e:
            self.logger.log_error("Snowflake", f"Error creating stage: {str(e)}")
            
    def create_table_from_df(self, table_name, df, mysql_conn):
        """Create table in Snowflake using MySQL types from type-mapping.yml"""
        try:
            table_name = table_name.upper()
            
            # Get MySQL column types
            mysql_types = self.get_mysql_column_types(mysql_conn, table_name)
            if not mysql_types:
                raise ValueError(f"Could not get MySQL types for table {table_name}")
            
            # Log column mapping process
            self.logger.log_info("Type Mapping", f"Starting column mapping for table '{table_name}':")
            
            columns = []
            for col in df.columns:
                col_upper = col.upper()
                
                if col_upper not in mysql_types:
                    raise ValueError(f"Column {col_upper} not found in MySQL table schema")
                    
                mysql_type = mysql_types[col_upper]
                sf_type = self.get_snowflake_type(mysql_type)
                
                # Only log if there's a type mapping issue (logging happens in get_snowflake_type)
                columns.append(f'"{col_upper}" {sf_type}')
            
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                {', '.join(columns)}
            )
            """
            
            self.logger.log_query("Snowflake", create_table_sql)
            cursor = self.connection.cursor()
            cursor.execute(create_table_sql)
            
            # Log successful table creation with final schema
            self.logger.log_info("Snowflake", f"Successfully created table '{table_name}' with schema:")
            for col in columns:
                self.logger.log_info("Snowflake", f"  - {col}")
            
            return True
        except Exception as e:
            self.logger.log_error("Snowflake", f"Error creating table {table_name}: {str(e)}")
            return False
    
    def _split_and_save_df(self, table_name, df):
        """Split dataframe into chunks and save to files"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_paths = []
        
        # Calculate number of chunks
        num_chunks = len(df) // self.chunk_size + (1 if len(df) % self.chunk_size else 0)
        
        for i in range(num_chunks):
            start_idx = i * self.chunk_size
            end_idx = min((i + 1) * self.chunk_size, len(df))
            chunk_df = df.iloc[start_idx:end_idx]
            
            # Create file path
            file_path = os.path.join(
                self.data_dir, 
                f"{table_name}_{timestamp}_part_{i+1:03d}.csv"
            )
            
            # Save chunk to CSV
            chunk_df.to_csv(file_path, index=False, header=True)
            file_paths.append(file_path)
            
            self.logger.log_info(
                "Snowflake", 
                f"Created file part {i+1}/{num_chunks} for {table_name}: {file_path}"
            )
            
        return file_paths
            
    def load_data(self, table_name, df):
        try:
            table_name = table_name.upper()
            df.columns = df.columns.str.upper()
            
            # Split dataframe and save to files
            file_paths = self._split_and_save_df(table_name, df)
            
            cursor = self.connection.cursor()
            total_files = len(file_paths)
            
            for idx, file_path in enumerate(file_paths, 1):
                try:
                    # Put file to stage
                    put_sql = f"PUT file://{file_path} @{self.stage_name}"
                    self.logger.log_query("Snowflake", put_sql)
                    cursor.execute(put_sql)
                    
                    # Copy into table
                    copy_sql = f"""
                    COPY INTO "{table_name}"
                    FROM @{self.stage_name}/{os.path.basename(file_path)}
                    FILE_FORMAT = (TYPE = CSV PARSE_HEADER = TRUE)
                    ON_ERROR = ABORT_STATEMENT
                    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
                    PURGE = TRUE
                    """
                    self.logger.log_query("Snowflake", copy_sql)
                    cursor.execute(copy_sql)
                    
                    self.logger.log_info(
                        "Snowflake", 
                        f"Loaded part {idx}/{total_files} to table: {table_name}"
                    )
                    
                finally:
                    # Clean up the file after loading
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        self.logger.log_info("Snowflake", f"Cleaned up file: {file_path}")
            
            return True
                
        except Exception as e:
            self.logger.log_error("Snowflake", f"Error loading data to {table_name}: {str(e)}")
            return False
            
    def truncate_table(self, table_name):
        try:
            table_name = table_name.upper()
            truncate_sql = f'TRUNCATE TABLE IF EXISTS "{table_name}"'
            self.logger.log_query("Snowflake", truncate_sql)
            cursor = self.connection.cursor()
            cursor.execute(truncate_sql)
            return True
        except Exception as e:
            self.logger.log_error("Snowflake", f"Error truncating table {table_name}: {str(e)}")
            return False
            
    def _load_type_mapping(self):
        """Load type mapping configuration from YAML file"""
        try:
            with open('type-mapping.yml', 'r') as f:
                self.type_mapping = yaml.safe_load(f)
                
                # Log loaded mappings
                self.logger.log_info("Type Mapping", "Loaded type mappings from YAML:")
                for base_type, mappings in self.type_mapping.items():
                    if 'snowflake' in mappings:
                        self.logger.log_info("Type Mapping", 
                            f"  Base type: {base_type} -> Snowflake: {mappings['snowflake']}")
                
                return self.type_mapping
        except Exception as e:
            self.logger.log_error("Snowflake", f"Error loading type mapping: {str(e)}")
            return {}

    def get_snowflake_type(self, mysql_type):
        """Get Snowflake type from type mapping based on MySQL type"""
        base_type = mysql_type.split('(')[0].lower()
        
        # Search for the base type in the mapping
        if base_type in self.type_mapping:
            snowflake_type = self.type_mapping[base_type]['snowflake']
            return snowflake_type
        else:
            # Only log when type mapping is not found
            self.logger.log_warning("Type Mapping", 
                f"No mapping found for type: {base_type}, defaulting to VARCHAR")
            return 'VARCHAR'

    def close(self):
        if self.connection:
            self.connection.close()
            self.logger.log_info("Snowflake", "Connection closed")

    def get_table_columns(self, table_name):
        """Get column names and types for a Snowflake table"""
        try:
            table_name = table_name.upper()
            cursor = self.connection.cursor()
            
            # Query to get column information
            query = f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{table_name}'
            AND TABLE_SCHEMA = '{self.schema}'
            ORDER BY ORDINAL_POSITION
            """
            
            cursor.execute(query)
            columns = {row[0].upper(): row[1] for row in cursor.fetchall()}
            return columns
        except Exception as e:
            self.logger.log_error("Snowflake", f"Error getting columns for table {table_name}: {str(e)}")
            return None

    def compare_table_columns(self, table_name, df):
        """Compare columns between DataFrame and existing Snowflake table"""
        try:
            table_name = table_name.upper()
            
            # First check if table exists
            if not self.table_exists(table_name):
                return True, None  # Table doesn't exist, no comparison needed
            
            # Get Snowflake columns
            sf_columns = self.get_table_columns(table_name)
            if sf_columns is None:
                return False, "Error getting Snowflake table columns"
            
            # Get column names only (ignore data types)
            df_columns = set(col.upper() for col in df.columns)
            sf_columns = set(sf_columns.keys())
            
            # Find differences in column names only
            differences = {
                'missing_in_sf': list(df_columns - sf_columns),
                'missing_in_df': list(sf_columns - df_columns)
            }
            
            has_differences = any(differences.values())
            
            if has_differences:
                diff_message = "Column differences found:\n"
                if differences['missing_in_sf']:
                    diff_message += f"- Columns in MySQL but not in Snowflake: {', '.join(differences['missing_in_sf'])}\n"
                if differences['missing_in_df']:
                    diff_message += f"- Columns in Snowflake but not in MySQL: {', '.join(differences['missing_in_df'])}\n"
                
                return False, diff_message
            
            return True, None
            
        except Exception as e:
            self.logger.log_error("Snowflake", f"Error comparing columns for table {table_name}: {str(e)}")
            return False, str(e)

    def get_mysql_column_types(self, mysql_conn, table_name):
        """Get actual MySQL column types"""
        try:
            query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE UPPER(TABLE_NAME) = '{table_name}'
            AND TABLE_CATALOG = '{mysql_conn.database}'
            ORDER BY ORDINAL_POSITION
            """
            # Log the query being executed
            self.logger.log_info("MySQL", f"Executing query to get column types:\n{query}")
            
            cursor = mysql_conn.connection.cursor()
            cursor.execute(query)
            
            # Process results
            column_types = {}
            for row in cursor.fetchall():
                col_name, data_type = row
                column_types[col_name.upper()] = data_type.lower()
            
            return column_types
            
        except Exception as e:
            self.logger.log_error("MySQL", f"Error getting column types: {str(e)}")
            return {}

    def table_exists(self, table_name):
        """Check if table exists in Snowflake"""
        try:
            table_name = table_name.upper()
            cursor = self.connection.cursor()
            query = f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = '{table_name}'
            AND TABLE_SCHEMA = '{self.schema}'
            """
            cursor.execute(query)
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            self.logger.log_error("Snowflake", f"Error checking table existence: {str(e)}")
            return False 