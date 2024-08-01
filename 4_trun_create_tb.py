#!/usr/bin/env python3
import psycopg2
import os
import json
#import pgvector
import math
from psycopg2.extras import execute_values
#from pgvector.psycopg2 import register_vector

      
# Establish connection to PostgreSQL database
################################################
#This requires superuser 
################################################
try:
    connection = psycopg2.connect(
#        user="vectorsmith",
        user="postgres",
        password="yourownpassword",
        host="localhost",
        port="5432",
        database="vectordb"
    )
    cursor = connection.cursor()
    print("Connected to PostgreSQL database!")
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL:", error)
 
grants = '''
GRANT ALL PRIVILEGES ON SCHEMA public TO vectorsmith;
GRANT CREATE ON SCHEMA public TO vectorsmith;
'''

try:
    cursor.execute(grants)
    connection.commit()
    print("Grant ran successfully!")
except (Exception, psycopg2.Error) as error:
    print("Error creating table:", error)

current_user = '''
SELECT current_user AS "Current User",
       session_user AS "Session User",
       usename AS "Role Name",
       usecreatedb AS "Can Create DB",
       usesuper AS "Is Superuser",
       valuntil AS "Password Expiry Date"
FROM pg_user
WHERE usename = current_user;
'''
try:
    # Execute the query
    cursor.execute(current_user)

    # Fetch all rows from the result
    result = cursor.fetchall()

    # Print the result
    for row in result:
        print(row)
except (Exception, psycopg2.Error) as error:
    print("Error executing query:", error)

# Create a table
create_table_query = '''
            TRUNCATE TABLE mlops;
            TRUNCATE TABLE mlops_p1;
            TRUNCATE TABLE mlops_p2;
            
            TRUNCATE TABLE mlops;

            CREATE TABLE IF NOT EXISTS mlops (
                id SERIAL,
                chunk TEXT,
                embedding BYTEA,
                PRIMARY KEY (id, chunk)
            ) PARTITION BY RANGE (id);

            -- Create partitions for the table
            CREATE TABLE IF NOT EXISTS mlops_p1 PARTITION OF mlops
                FOR VALUES FROM (1) TO (1000);

            CREATE TABLE IF NOT EXISTS mlops_p2 PARTITION OF mlops
                FOR VALUES FROM (1000) TO (2000);

                   '''



try:
    cursor.execute(create_table_query)
    connection.commit()
    print("mlops tables created successfully!")
except (Exception, psycopg2.Error) as error:
    print("Error creating table:", error)


