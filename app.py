import streamlit as st
from database import DatabaseConnection
from snowflake_db import SnowflakeConnection
import json
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

def load_mysql_credentials():
    try:
        with open('creds-mysql.json', 'r') as f:
            return json.load(f)['databases']
    except FileNotFoundError:
        st.error("creds-mysql.json file not found!")
        return {}
    except json.JSONDecodeError:
        st.error("Invalid JSON format in creds-mysql.json!")
        return {}

def load_snowflake_credentials():
    try:
        with open('creds-snowflake.json', 'r') as f:
            return json.load(f)['connections']
    except FileNotFoundError:
        st.error("creds-snowflake.json file not found!")
        return {}
    except json.JSONDecodeError:
        st.error("Invalid JSON format in creds-snowflake.json!")
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
    
    # Show summary of selection
    if selected_tables:
        st.markdown("---")
        st.markdown("#### Selected Tables:")
        for table, load_type in selected_tables.items():
            st.markdown(f"- **{table}** ({load_type})")
    
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
                # Log the mismatch but continue with migration
                snowflake_conn.logger.log_info("Migration", 
                    f"Proceeding with migration despite column mismatch for {table}:\n{diff_message}")
            
            # For truncate and load, drop existing table
            if load_type == "Truncate and Load":
                snowflake_conn.truncate_table(table)
                
            # Create table if it doesn't exist
            if snowflake_conn.create_table_from_df(table, df):
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
    st.title("MySQL > Snowflake Migration Tool")
    
    # Database connection settings
    st.sidebar.header("Database Connections")
    
    # MySQL Connection
    st.sidebar.subheader("MySQL Connection")
    mysql_creds = load_mysql_credentials()
    if mysql_creds:
        selected_db = st.sidebar.selectbox(
            "Select MySQL Configuration",
            options=list(mysql_creds.keys())
        )
        if selected_db:
            db_config = mysql_creds[selected_db]
            
            # Display the selected configuration (hide password)
            st.sidebar.text(f"Host: {db_config['host']}")
            st.sidebar.text(f"User: {db_config['user']}")
            st.sidebar.text(f"Database: {db_config['database']}")
            
            # Connect button for MySQL
            if st.sidebar.button("Connect to MySQL"):
                db = DatabaseConnection(**db_config)
                if db.connect():
                    st.session_state['db'] = db
                    st.success("Connected to MySQL database successfully!")
                else:
                    st.error("Failed to connect to MySQL database")
    else:
        st.sidebar.error("No MySQL configurations found!")

    # Snowflake Connection in Sidebar
    st.sidebar.subheader("Snowflake Connection")
    sf_creds = load_snowflake_credentials()
    if sf_creds:
        selected_sf = st.sidebar.selectbox(
            "Select Snowflake Configuration",
            options=list(sf_creds.keys())
        )
        if selected_sf:
            sf_config = sf_creds[selected_sf]
            
            # Display the selected configuration (hide password)
            st.sidebar.text(f"Account: {sf_config['account']}")
            st.sidebar.text(f"Database: {sf_config['database']}")
            st.sidebar.text(f"Schema: {sf_config['schema']}")
            st.sidebar.text(f"Warehouse: {sf_config['warehouse']}")
            
            # Connect button for Snowflake
            if st.sidebar.button("Connect to Snowflake"):
                sf = SnowflakeConnection(**sf_config)
                if sf.connect():
                    st.session_state['sf'] = sf
                    st.success("Connected to Snowflake successfully!")
                else:
                    st.error("Failed to connect to Snowflake")
    else:
        st.sidebar.error("No Snowflake configurations found!")
    
    # Main content area - Table selection and migration
    if 'db' in st.session_state:
        tables = st.session_state['db'].get_tables()
        
        if tables:
            # Display table selection with checkboxes and load type options
            selected_tables = display_table_selection(tables)
            
            # Show selected tables count and migration button
            if selected_tables:
                st.write(f"Selected {len(selected_tables)} tables")
                
                # Configuration for parallel processing - only show if multiple tables
                max_workers = 1  # Default for single table
                if len(selected_tables) > 1:
                    max_workers = st.slider(
                        "Maximum parallel migrations", 
                        min_value=1, 
                        max_value=min(len(selected_tables), 10), 
                        value=min(len(selected_tables), 3)
                    )
                
                # Show migration button only if Snowflake is connected
                if 'sf' in st.session_state:
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
                                    st.session_state['db'],
                                    st.session_state['sf'],
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
        if 'db' in st.session_state:
            st.session_state['db'].close()
        if 'sf' in st.session_state:
            st.session_state['sf'].close()

    # Register the cleanup function
    st.session_state['cleanup'] = cleanup

if __name__ == "__main__":
    main() 