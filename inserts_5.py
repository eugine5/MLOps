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
        (2000, 3000, 'mlops_p3'),
        (3000, 4000, 'mlops_p4'),
        (4000, 5000, 'mlops_p5'),
        (5000, 6000, 'mlops_p6'),
        (6000, 7000, 'mlops_p7'),
        (7000, 8000, 'mlops_p8'),
        (8000, 9000, 'mlops_p9'),
        (9000, 10000, 'mlops_p10'),
        (10000, 11000, 'mlops_p11'),
        (11000, 12000, 'mlops_p12'),
        (12000, 13000, 'mlops_p13'),
        (13000, 14000, 'mlops_p14'),
        (14000, 15000, 'mlops_p15'),
        (15000, 16000, 'mlops_p16'),
        (16000, 17000, 'mlops_p17'),
        (17000, 18000, 'mlops_p18'),
        (18000, 19000, 'mlops_p19'),
        (19000, 20000, 'mlops_p20'),
        (20000, 21000, 'mlops_p21'),
        (21000, 22000, 'mlops_p22'),
        (22000, 23000, 'mlops_p23'),
        (23000, 24000, 'mlops_p24'),
        (24000, 25000, 'mlops_p25'),
        (25000, 26000, 'mlops_p26'),
        (26000, 27000, 'mlops_p27'),
        (27000, 28000, 'mlops_p28'),
        (28000, 29000, 'mlops_p29'),
        (29000, 30000, 'mlops_p30'),
        (30000, 31000, 'mlops_p31'),
        (31000, 32000, 'mlops_p32'),
        (32000, 33000, 'mlops_p33'),
        (33000, 34000, 'mlops_p34'),
        (34000, 35000, 'mlops_p35'),
        (35000, 36000, 'mlops_p36'),
        (36000, 37000, 'mlops_p37'),
        (37000, 38000, 'mlops_p38'),
        (38000, 39000, 'mlops_p39'),
        (39000, 40000, 'mlops_p40'),
        (40000, 41000, 'mlops_p41'),
        (41000, 42000, 'mlops_p42'),
        (42000, 43000, 'mlops_p43'),
        (43000, 44000, 'mlops_p44'),
        (44000, 45000, 'mlops_p45'),
        (45000, 46000, 'mlops_p46'),
        (46000, 47000, 'mlops_p47'),
        (47000, 48000, 'mlops_p48'),
        (48000, 49000, 'mlops_p49'),
        (49000, 50000, 'mlops_p50'),
        (50000, 51000, 'mlops_p51'),
        (51000, 52000, 'mlops_p52'),
        (52000, 53000, 'mlops_p53'),
        (53000, 54000, 'mlops_p54'),
        (54000, 55000, 'mlops_p55'),
        (55000, 56000, 'mlops_p56'),
        (56000, 57000, 'mlops_p57'),
        (57000, 58000, 'mlops_p58'),
        (58000, 59000, 'mlops_p59'),
        (59000, 60000, 'mlops_p60'),
        (60000, 61000, 'mlops_p61'),
        (61000, 62000, 'mlops_p62'),
        (62000, 63000, 'mlops_p63'),
        (63000, 64000, 'mlops_p64'),
        (64000, 65000, 'mlops_p65'),
        (65000, 66000, 'mlops_p66'),
        (66000, 67000, 'mlops_p67'),
        (67000, 68000, 'mlops_p68'),
        (68000, 69000, 'mlops_p69'),
        (69000, 70000, 'mlops_p70'),
        (70000, 71000, 'mlops_p71'),
        (71000, 72000, 'mlops_p72'),
        (72000, 73000, 'mlops_p73'),
        (73000, 74000, 'mlops_p74'),
        (74000, 75000, 'mlops_p75'),
        (75000, 76000, 'mlops_p76'),
        (76000, 77000, 'mlops_p77'),
        (77000, 78000, 'mlops_p78'),
        (78000, 79000, 'mlops_p79'),
        (79000, 80000, 'mlops_p80'),
        (80000, 81000, 'mlops_p81'),
        (81000, 82000, 'mlops_p82'),
        (82000, 83000, 'mlops_p83'),
        (83000, 84000, 'mlops_p84'),
        (84000, 85000, 'mlops_p85'),
        (85000, 86000, 'mlops_p86'),
        (86000, 87000, 'mlops_p87'),
        (87000, 88000, 'mlops_p88'),
        (88000, 89000, 'mlops_p89'),
        (89000, 90000, 'mlops_p90'),
        (90000, 91000, 'mlops_p91'),
        (91000, 92000, 'mlops_p92'),
        (92000, 93000, 'mlops_p93'),
        (93000, 94000, 'mlops_p94'),
        (94000, 95000, 'mlops_p95'),
        (95000, 96000, 'mlops_p96'),
        (96000, 97000, 'mlops_p97'),
        (97000, 98000, 'mlops_p98'),
        (98000, 99000, 'mlops_p99'),
        (99000, 100000, 'mlops_p100'),
        (100000, 101000, 'mlops_p101'),
        (101000, 102000, 'mlops_p102'),
        (102000, 103000, 'mlops_p103'),
        (103000, 104000, 'mlops_p104'),
        (104000, 105000, 'mlops_p105'),
        (105000, 106000, 'mlops_p106'),
        (106000, 107000, 'mlops_p107'),
        (107000, 108000, 'mlops_p108'),
        (108000, 109000, 'mlops_p109'),
        (109000, 110000, 'mlops_p110'),
        (110000, 111000, 'mlops_p111'),
        (111000, 112000, 'mlops_p112'),
        (112000, 113000, 'mlops_p113'),
        (113000, 114000, 'mlops_p114'),
        (114000, 115000, 'mlops_p115'),
        (115000, 116000, 'mlops_p116'),
        (116000, 117000, 'mlops_p117'),
        (117000, 118000, 'mlops_p118'),
        (118000, 119000, 'mlops_p119'),
        (119000, 120000, 'mlops_p120'),
        (120000, 121000, 'mlops_p121'),
        (121000, 122000, 'mlops_p122'),
        (122000, 123000, 'mlops_p123'),
        (123000, 124000, 'mlops_p124'),
        (124000, 125000, 'mlops_p125'),
        (125000, 126000, 'mlops_p126'),
        (126000, 127000, 'mlops_p127'),
        (127000, 128000, 'mlops_p128'),
        (128000, 129000, 'mlops_p129'),
        (129000, 130000, 'mlops_p130'),
        (130000, 131000, 'mlops_p131'),
        (131000, 132000, 'mlops_p132'),
        (132000, 133000, 'mlops_p133'),
        (133000, 134000, 'mlops_p134'),
        (134000, 135000, 'mlops_p135'),
        (135000, 136000, 'mlops_p136'),
        (136000, 137000, 'mlops_p137'),
        (137000, 138000, 'mlops_p138'),
        (138000, 139000, 'mlops_p139'),
        (139000, 140000, 'mlops_p140')
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
    'password': "supportvectors.123",
    'host': "localhost",
    'port': "5432",
    'database': "vectordb"
}

# Directory containing CSV files
csv_directory = '/home/vas/Documents/llava3/yjc_ragscale/ragscale/src/processed'
csv_pattern = 'embedded_texts_*.csv'

# Execute the insertion
insert_from_multiple_csv_files(csv_directory, csv_pattern, db_params)
