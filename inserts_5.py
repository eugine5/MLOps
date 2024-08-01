#!/usr/bin/env python3
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import glob
import os
from concurrent.futures import ProcessPoolExecutor

def convert_numpy_to_python(records):
    """
    Convert numpy data types to native Python data types.
    """
    converted_records = []
    for record in records:
        converted_record = tuple(item.item() if isinstance(item, np.generic) else item for item in record)
        converted_records.append(converted_record)
    return converted_records

def insert_partition_data(partition_data, partition_table, db_params):
    """
    Insert partition data into the specified partition table.
    """
    if not partition_data.empty:
        records = partition_data.to_records(index=False)
        records = convert_numpy_to_python(records)  # Convert numpy types to native Python types

        # Generate the SQL statement for insertion
        sql = f"""
        INSERT INTO {partition_table} (id, chunk, embedding) 
        VALUES %s
        """
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        try:
            execute_values(cursor, sql, records)
            conn.commit()
        except Exception as e:
            print(f"Error inserting data into {partition_table}: {e}")
        finally:
            cursor.close()
            conn.close()

def insert_data_into_partitioned_table(csv_file_path, db_params):
    """
    Insert data from a CSV file into the partitioned PostgreSQL tables.
    """
    # Read the CSV file into a DataFrame
    data = pd.read_csv(csv_file_path)
    
    # Check if 'id' column exists and add if necessary
    if 'id' not in data.columns:
        data.insert(0, 'id', range(1, len(data) + 1))
    
    # Ensure 'id' column is of integer type
    data['id'] = data['id'].astype(int)

    # Determine the partition ranges and corresponding table names
    partition_ranges = [
         (1, 1000, 'mlops_p1'),
        (1000, 2000, 'mlops_p2'),
        (2000, 3000, 'mlops_p3')
    ]

    # Use ProcessPoolExecutor to insert data into partitions in parallel
    with ProcessPoolExecutor() as executor:
        futures = []
        for start, end, partition_table in partition_ranges:
            partition_data = data[(data['id'] >= start) & (data['id'] < end)]
            futures.append(executor.submit(insert_partition_data, partition_data, partition_table, db_params))
        
        # Wait for all tasks to complete
        for future in futures:
            future.result()

def insert_from_multiple_csv_files(csv_directory, pattern, db_params):
    """
    Insert data from multiple CSV files matching a pattern into the partitioned PostgreSQL tables.
    """
    csv_file_paths = glob.glob(os.path.join(csv_directory, pattern))
    
    # Use ProcessPoolExecutor to process multiple CSV files in parallel
    with ProcessPoolExecutor() as executor:
        futures = []
        for csv_file_path in csv_file_paths:
            print(f"Processing file: {csv_file_path}")
            futures.append(executor.submit(insert_data_into_partitioned_table, csv_file_path, db_params))
        
        # Wait for all tasks to complete
        for future in futures:
            future.result()

# Database connection parameters
db_params = {
    'user': "postgres",
    'password': "yourownpassword",
    'host': "localhost",
    'port': "5432",
    'database': "vectordb"
}

# Directory containing CSV files
csv_directory = '/home/vas/Documents/llava3/yjc_ragscale/ragscale/src/processed'
csv_pattern = 'embedded_texts_*.csv'

# Execute the insertion
insert_from_multiple_csv_files(csv_directory, csv_pattern, db_params)
