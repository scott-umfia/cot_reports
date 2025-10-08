#!/usr/bin/env python3
"""
COT Data Importer
Imports Commitments of Traders (COT) data from zip files into MySQL database.
"""

import argparse
import csv
import zipfile
import sys
import os
from pathlib import Path
import mysql.connector
from mysql.connector import Error

# historical CFTC COT data can be found at
# https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm
# https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm



# Define table configurations
TABLE_CONFIGS = {
    'cot_disaggregated_fut': {
        'table_name': 'cot_disaggregated_fut',
        'date_column': 'As_of_Date_Form_MM_DD_YYYY'
    },
    'cot_disaggregated_fut_opt': {
        'table_name': 'cot_disaggregated_fut_opt',
        'date_column': 'As_of_Date_Form_MM_DD_YYYY'
    },
    'cot_index_supplement': {
        'table_name': 'cot_index_supplement',
        'date_column': 'As_of_Date_In_Form_MM_DD_YYYY'
    },
    'cot_legacy_fut': {
        'table_name': 'cot_legacy_fut',
        'date_column': 'Report_Date_as_MM_DD_YYYY'
    },
    'cot_legacy_fut_opt': {
        'table_name': 'cot_legacy_fut_opt',
        'date_column': 'Report_Date_as_MM_DD_YYYY'
    }
}


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Import COT data from zip file into MySQL database'
    )
    parser.add_argument(
        'zipfile',
        help='Path to the zip file containing COT data'
    )
    parser.add_argument(
        'table',
        choices=list(TABLE_CONFIGS.keys()),
        help='Target table name'
    )
    parser.add_argument(
        '--host',
        default='localhost',
        help='MySQL host (default: localhost)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=3306,
        help='MySQL port (default: 3306)'
    )
    parser.add_argument(
        '--user',
        default='root',
        help='MySQL user (default: root)'
    )
    parser.add_argument(
        '--password',
        required=True,
        help='MySQL password'
    )
    parser.add_argument(
        '--database',
        default='historical',
        help='Database name (default: historical)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of rows per batch insert (default: 1000)'
    )
    
    return parser.parse_args()


def extract_txt_from_zip(zip_path):
    """Extract the single txt file from the zip archive."""
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Zip file not found: {zip_path}")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        txt_files = [f for f in zip_ref.namelist() if f.endswith('.txt')]
        
        if len(txt_files) == 0:
            raise ValueError("No .txt file found in the zip archive")
        if len(txt_files) > 1:
            raise ValueError(f"Multiple .txt files found in zip. Expected 1, found {len(txt_files)}")
        
        txt_file = txt_files[0]
        print(f"Extracting {txt_file}...")
        
        # Extract to temporary location
        zip_ref.extract(txt_file, path='/tmp')
        return os.path.join('/tmp', txt_file)


def clean_value(value):
    """Clean and convert values for database insertion."""
    if value is None or value == '':
        return None
    
    # Remove leading/trailing whitespace
    value = value.strip()
    
    # Handle empty strings after stripping
    if value == '' or value == '.':
        return None
    
    return value


def create_connection(host, port, user, password, database):
    """Create database connection."""
    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            print(f"Successfully connected to MySQL database '{database}'")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        sys.exit(1)


def batch_insert(connection, table_name, columns, rows, batch_size=1000):
    """Insert rows in batches."""
    cursor = connection.cursor()
    total_rows = len(rows)
    inserted = 0
    
    # Prepare the INSERT statement
    placeholders = ', '.join(['%s'] * len(columns))
    column_names = ', '.join([f'`{col}`' for col in columns])
    insert_query = f"INSERT IGNORE INTO `{table_name}` ({column_names}) VALUES ({placeholders})"
    if table_name == "cot_legacy_fut" or table_name == "cot_legacy_fut_opt" or table_name == "cot_index_supplement":
        insert_query = f"INSERT IGNORE INTO `{table_name}` VALUES ({placeholders})"
    
    try:
        for i in range(0, total_rows, batch_size):
            batch = rows[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
            inserted += len(batch)
            print(f"Inserted {inserted}/{total_rows} rows ({inserted/total_rows*100:.1f}%)")
        
        print(f"Successfully inserted {inserted} rows into {table_name}")
        
    except Error as e:
        connection.rollback()
        print(f"Error during batch insert: {e}")
        raise
    finally:
        cursor.close()


def import_data(txt_file, connection, table_config, batch_size):
    """Read and import data from txt file."""
    table_name = table_config['table_name']
    
    print(f"Reading data from {txt_file}...")
    
    rows = []
    columns = None
    row_count = 0
    
    with open(txt_file, 'r', encoding='utf-8') as f:
        # Use csv.reader to handle quoted fields properly
        reader = csv.reader(f, delimiter=',', quotechar='"')
        
        for idx, row in enumerate(reader):
            if idx == 0:
                # First row is the header
                columns = [col.strip() for col in row]
                columns = [col.replace("-", "_") for col in columns]  # for columns with YYYY-MM-DD text
                #if table_name == 'cot_legacy_fut':
                #    columns = [col.replace(" ", "_") for col in columns]
                #    columns = [col.replace("-", "_") for col in columns]
                #    columns = [col.replace("(", "") for col in columns]
                #    columns = [col.replace(")", "") for col in columns]
                #    columns = [col.replace("LT =", "LE") for col in columns]
                #    columns = [col.replace("Noncommercial", "NonComm") for col in columns]
                #    columns = [col.replace("Total_Reportable", "Tot_Rept") for col in columns]
                #    columns = [col.replace("Nonreportable", "NonRept") for col in columns]
                #    columns = [col.replace("Commercial", "Comm") for col in columns]
                #    columns = [col.replace("Spreading", "Spread") for col in columns]
                #    columns = [col.replace("%", "Pct") for col in columns]
                #    columns = [col.replace("Pct_of_Open_Interest_OI_All", "Pct_of_Open_Interest_All") for col in columns]
                print(f"Found {len(columns)} columns")
                continue
            
            # Clean and convert values
            cleaned_row = [clean_value(val) for val in row]
            rows.append(cleaned_row)
            row_count += 1
            
            # Progress indicator
            if row_count % 10000 == 0:
                print(f"Read {row_count} rows...")
    
    print(f"Read {row_count} total data rows")
    
    if not rows:
        print("No data rows found to import")
        return
    
    # Perform batch insert
    print(f"Inserting data into {table_name}...")
    batch_insert(connection, table_name, columns, rows, batch_size)


def main():
    """Main execution function."""
    args = parse_arguments()
    
    try:
        # Extract txt file from zip
        txt_file = extract_txt_from_zip(args.zipfile)
        
        # Get table configuration
        table_config = TABLE_CONFIGS[args.table]
        
        # Create database connection
        connection = create_connection(
            args.host,
            args.port,
            args.user,
            args.password,
            args.database
        )
        
        # Import data
        import_data(txt_file, connection, table_config, args.batch_size)
        
        # Cleanup
        connection.close()
        os.remove(txt_file)
        print("Import completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()