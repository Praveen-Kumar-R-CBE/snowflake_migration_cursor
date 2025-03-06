import streamlit as st
from mysql_db import MySQLConnection
from snowflake_db import SnowflakeConnection
import json
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from database_factory import DatabaseFactory

def load_credentials(db_type):
    """Load credentials for specified database type"""
    try:
        with open(f'creds-{db_type}.json', 'r') as f:
            return json.load(f)['connections']
    except FileNotFoundError:
        st.error(f"creds-{db_type}.json file not found!")
        return {}
    except json.JSONDecodeError:
        st.error(f"Invalid JSON format in creds-{db_type}.json!")
        return {}

def display_table_selection(tables):
    st.write("### Select Tables to Migrate")
    
    # Add "Select All" checkbox
    select_all = st.checkbox("Select All Tables")
    
    # Style for the grid
    st.markdown("""
        <style>
        .grid-container {
            display: grid;
            grid-template-columns: 80px 200px 200px;
            gap: 10px;
            align-items: center;
            margin-bottom: 10px;
        }
        .header {
            font-weight: bold;
            padding: 10px 0;
            border-bottom: 1px solid #ddd;
        }
        .row {
            padding: 5px 0;
            border-bottom: 1px solid #eee;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Create container for the grid
    container = st.container()
    
    # Headers
    with container:
        st.markdown(
            """
            <div class="grid-container header">
                <div>Select</div>
                <div>Table Name</div>
                <div>Load Type</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    selected_tables = {}
    
    # Create rows for each table
    for table in sorted(tables):
        with container:
            col1, col2, col3 = st.columns([1, 2.5, 2.5])
            
            # Checkbox for selection
            with col1:
                selected = st.checkbox("", key=f"select_{table}", value=select_all)
            
            # Table name
            with col2:
                st.markdown(f"<div style='padding-top: 5px;'>{table}</div>", unsafe_allow_html=True)
            
            # Load type selector
            with col3:
                if selected:
                    load_type = st.selectbox(
                        "",
                        options=["Truncate and Load", "Append"],
                        key=f"load_type_{table}",
                        label_visibility="collapsed"
                    )
                    selected_tables[table] = load_type
    
    return selected_tables

def migrate_table(table, load_type, mysql_conn, snowflake_conn, force_load=False):
    """Helper function to migrate a single table"""
    try:
        df = mysql_conn.query_table(table)
        if df is not None:
            # Compare table structures if table exists
            columns_match, diff_message = snowflake_conn.compare_table_columns(table, df)
            
            if not columns_match and not force_load:
                return table, False, f"Schema mismatch for {table}:\n{diff_message}"
            elif not columns_match and force_load:
                snowflake_conn.logger.log_info("Migration", 
                    f"Proceeding with migration despite column mismatch for {table}:\n{diff_message}")
            
            # For truncate and load, drop existing table
            if load_type == "Truncate and Load":
                snowflake_conn.truncate_table(table)
                
            # Create table if it doesn't exist - pass mysql_conn for type mapping
            if snowflake_conn.create_table_from_df(table, df, mysql_conn):
                # Load data
                if snowflake_conn.load_data(table, df):
                    return table, True, f"Successfully migrated {table} ({load_type})"
                else:
                    return table, False, f"Failed to load data for {table}"
            else:
                return table, False, f"Failed to create table {table} in Snowflake"
    except Exception as e:
        return table, False, f"Error migrating {table}: {str(e)}"

def main():
    st.title("Database Migration Tool")
    
    # Database connection settings
    st.sidebar.header("Database Connections")
    
    # Source Database Selection
    st.sidebar.subheader("Source Database")
    source_type = st.sidebar.selectbox(
        "Select Source Database Type",
        options=["MySQL", "SQL Server"]
    )
    
    # Load credentials for selected source database
    source_creds = load_credentials(source_type.lower().replace(" ", ""))
    if source_creds:
        selected_source = st.sidebar.selectbox(
            f"Select {source_type} Configuration",
            options=list(source_creds.keys()),
            key="source_config"
        )
        
        if selected_source:
            source_config = source_creds[selected_source]
            
            # Display the selected configuration (hide password)
            for key, value in source_config.items():
                if key != 'password':
                    st.sidebar.text(f"{key}: {value}")
            
            # Connect button for source
            if st.sidebar.button(f"Connect to {source_type}"):
                source_db = DatabaseFactory.create_connection(
                    source_type.lower().replace(" ", ""),  # Normalize the type string
                    **source_config
                )
                if source_db.connect():
                    st.session_state['source_db'] = source_db
                    st.success(f"Connected to {source_type} successfully!")
                else:
                    st.error(f"Failed to connect to {source_type}")
    
    # Target Database Selection
    st.sidebar.subheader("Target Database")
    target_type = st.sidebar.selectbox(
        "Select Target Database Type",
        options=["Snowflake"]  # Can add more target databases here
    )
    
    # Load Snowflake credentials
    target_creds = load_credentials(target_type.lower())
    if target_creds:
        selected_target = st.sidebar.selectbox(
            f"Select {target_type} Configuration",
            options=list(target_creds.keys()),
            key="target_config"
        )
        
        if selected_target:
            target_config = target_creds[selected_target]
            
            # Display the selected configuration (hide password)
            for key, value in target_config.items():
                if key != 'password':
                    st.sidebar.text(f"{key}: {value}")
            
            # Connect button for target
            if st.sidebar.button(f"Connect to {target_type}"):
                target_db = DatabaseFactory.create_connection(target_type, **target_config)
                if target_db.connect():
                    st.session_state['target_db'] = target_db
                    st.success(f"Connected to {target_type} successfully!")
                else:
                    st.error(f"Failed to connect to {target_type}")
    
    # Main content area - Table selection and migration
    if 'source_db' in st.session_state:
        tables = st.session_state['source_db'].get_tables()
        
        if tables:
            # Display table selection with checkboxes and load type options
            selected_tables = display_table_selection(tables)
            
            # Show selected tables count and migration button
            if selected_tables:
                # Configuration for parallel processing - only show if multiple tables
                max_workers = 1  # Default for single table
                if len(selected_tables) > 1:
                    max_parallel = min(len(selected_tables), 4)  # Maximum of 4 or number of tables
                    max_workers = st.number_input(
                        "Maximum parallel migrations",
                        min_value=1,
                        max_value=max_parallel,
                        value=min(max_parallel, 3)  # Default to 3 or less if fewer tables
                    )
                
                # Show migration button only if Snowflake is connected
                if 'target_db' in st.session_state:
                    # Add force load option
                    force_load = st.checkbox("Force load tables (ignore column mismatches)", 
                                          help="Enable this to proceed with migration even if table structures don't match")
                    
                    if st.button("Start Migration"):
                        progress_placeholder = st.empty()
                        status_placeholders = {table: st.empty() for table in selected_tables}
                        
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            # Submit all migration tasks
                            future_to_table = {
                                executor.submit(
                                    migrate_table, 
                                    table, 
                                    load_type,
                                    st.session_state['source_db'],
                                    st.session_state['target_db'],
                                    force_load
                                ): table 
                                for table, load_type in selected_tables.items()
                            }
                            
                            # Track progress
                            completed = 0
                            total = len(selected_tables)
                            
                            # Process completed migrations
                            for future in as_completed(future_to_table):
                                table = future_to_table[future]
                                try:
                                    table, success, message = future.result()
                                    completed += 1
                                    
                                    # Update progress - fix for progress value
                                    progress = completed / total
                                    if progress <= 1.0:  # Ensure progress doesn't exceed 1.0
                                        progress_placeholder.progress(progress)
                                    
                                    # Update status
                                    if success:
                                        status_placeholders[table].success(message)
                                    else:
                                        status_placeholders[table].error(message)
                                        
                                except Exception as e:
                                    status_placeholders[table].error(f"Error in {table}: {str(e)}")
                                
                        # Final progress message
                        if completed == total:
                            progress_placeholder.success(f"Migration completed! {completed}/{total} tables processed")
                else:
                    st.warning("Please connect to Snowflake before starting migration")
        else:
            st.warning("No tables found in the database")

    # Close connections when the app is done
    def cleanup():
        if 'source_db' in st.session_state:
            st.session_state['source_db'].close()
        if 'target_db' in st.session_state:
            st.session_state['target_db'].close()

    # Register the cleanup function
    st.session_state['cleanup'] = cleanup

if __name__ == "__main__":
    main() 