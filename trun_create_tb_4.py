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
        password="supportvectors.123",
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

# # Create a table
# create_table_query = '''
#            drop table document cascade;
#            CREATE TABLE IF NOT EXISTS document (
#                file_name TEXT,
#                doc_type VARCHAR,
#                id SERIAL PRIMARY KEY
#            );
#            drop table chunks cascade;
#            CREATE TABLE IF NOT EXISTS chunks (
#                chunk TEXT,
#                embedding BYTEA, 
#                document_id INT,
#                id SERIAL PRIMARY KEY,
#                FOREIGN KEY (document_id) REFERENCES document(id)
#            );
#            drop table metadata cascade;
#            CREATE TABLE IF NOT EXISTS metadata (
#                meta_key TEXT,
#                value TEXT,
#                document_id INT,
#                id SERIAL PRIMARY KEY,
#                FOREIGN KEY (document_id) REFERENCES document(id)
#            );
#                    '''
# Create a table
create_table_query = '''
            TRUNCATE TABLE mlops;
            TRUNCATE TABLE mlops_p1;
            TRUNCATE TABLE mlops_p2;
            TRUNCATE TABLE mlops_p3;
            TRUNCATE TABLE mlops_p4;
            TRUNCATE TABLE mlops_p5;
            TRUNCATE TABLE mlops_p6;
            TRUNCATE TABLE mlops_p7;
            TRUNCATE TABLE mlops_p8;
            TRUNCATE TABLE mlops_p9;
            TRUNCATE TABLE mlops_p10;
            TRUNCATE TABLE mlops_p11;
            TRUNCATE TABLE mlops_p12;
            TRUNCATE TABLE mlops_p13;
            TRUNCATE TABLE mlops_p14;
            TRUNCATE TABLE mlops_p15;
            TRUNCATE TABLE mlops_p16;
            TRUNCATE TABLE mlops_p17;
            TRUNCATE TABLE mlops_p18;
            TRUNCATE TABLE mlops_p19;
            TRUNCATE TABLE mlops_p20;
            TRUNCATE TABLE mlops_p21;
            TRUNCATE TABLE mlops_p22;
            TRUNCATE TABLE mlops_p23;
            TRUNCATE TABLE mlops_p24;
            TRUNCATE TABLE mlops_p25;
            TRUNCATE TABLE mlops_p26;
            TRUNCATE TABLE mlops_p27;
            TRUNCATE TABLE mlops_p28;
            TRUNCATE TABLE mlops_p29;
            TRUNCATE TABLE mlops_p30;
            TRUNCATE TABLE mlops_p31;
            TRUNCATE TABLE mlops_p32;
            TRUNCATE TABLE mlops_p33;
            TRUNCATE TABLE mlops_p34;
            TRUNCATE TABLE mlops_p35;
            TRUNCATE TABLE mlops_p36;
            TRUNCATE TABLE mlops_p37;
            TRUNCATE TABLE mlops_p38;
            TRUNCATE TABLE mlops_p39;
            TRUNCATE TABLE mlops_p40;
            TRUNCATE TABLE mlops_p41;
            TRUNCATE TABLE mlops_p42;
            TRUNCATE TABLE mlops_p43;
            TRUNCATE TABLE mlops_p44;
            TRUNCATE TABLE mlops_p45;
            TRUNCATE TABLE mlops_p46;
            TRUNCATE TABLE mlops_p47;
            TRUNCATE TABLE mlops_p48;
            TRUNCATE TABLE mlops_p49;
            TRUNCATE TABLE mlops_p50;
            TRUNCATE TABLE mlops_p51;
            TRUNCATE TABLE mlops_p52;
            TRUNCATE TABLE mlops_p53;
            TRUNCATE TABLE mlops_p54;
            TRUNCATE TABLE mlops_p55;
            TRUNCATE TABLE mlops_p56;
            TRUNCATE TABLE mlops_p57;
            TRUNCATE TABLE mlops_p58;
            TRUNCATE TABLE mlops_p59;
            TRUNCATE TABLE mlops_p60;
            TRUNCATE TABLE mlops_p61;
            TRUNCATE TABLE mlops_p62;
            TRUNCATE TABLE mlops_p63;
            TRUNCATE TABLE mlops_p64;
            TRUNCATE TABLE mlops_p65;
            TRUNCATE TABLE mlops_p66;
            TRUNCATE TABLE mlops_p67;
            TRUNCATE TABLE mlops_p68;
            TRUNCATE TABLE mlops_p69;
            TRUNCATE TABLE mlops_p70;
            TRUNCATE TABLE mlops_p71;
            TRUNCATE TABLE mlops_p72;
            TRUNCATE TABLE mlops_p73;
            TRUNCATE TABLE mlops_p74;
            TRUNCATE TABLE mlops_p75;
            TRUNCATE TABLE mlops_p76;
            TRUNCATE TABLE mlops_p77;
            TRUNCATE TABLE mlops_p78;
            TRUNCATE TABLE mlops_p79;
            TRUNCATE TABLE mlops_p80;
            TRUNCATE TABLE mlops_p81;
            TRUNCATE TABLE mlops_p82;
            TRUNCATE TABLE mlops_p83;
            TRUNCATE TABLE mlops_p84;
            TRUNCATE TABLE mlops_p85;
            TRUNCATE TABLE mlops_p86;
            TRUNCATE TABLE mlops_p87;
            TRUNCATE TABLE mlops_p88;
            TRUNCATE TABLE mlops_p89;
            TRUNCATE TABLE mlops_p90;
            TRUNCATE TABLE mlops_p91;
            TRUNCATE TABLE mlops_p92;
            TRUNCATE TABLE mlops_p93;
            TRUNCATE TABLE mlops_p94;
            TRUNCATE TABLE mlops_p95;
            TRUNCATE TABLE mlops_p96;
            TRUNCATE TABLE mlops_p97;
            TRUNCATE TABLE mlops_p98;
            TRUNCATE TABLE mlops_p99;
            TRUNCATE TABLE mlops_p100;
            TRUNCATE TABLE mlops_p101;
            TRUNCATE TABLE mlops_p102;
            TRUNCATE TABLE mlops_p103;
            TRUNCATE TABLE mlops_p104;
            TRUNCATE TABLE mlops_p105;
            TRUNCATE TABLE mlops_p106;
            TRUNCATE TABLE mlops_p107;
            TRUNCATE TABLE mlops_p108;
            TRUNCATE TABLE mlops_p109;
            TRUNCATE TABLE mlops_p110;
            TRUNCATE TABLE mlops_p111;
            TRUNCATE TABLE mlops_p112;
            TRUNCATE TABLE mlops_p113;
            TRUNCATE TABLE mlops_p114;
            TRUNCATE TABLE mlops_p115;
            TRUNCATE TABLE mlops_p116;
            TRUNCATE TABLE mlops_p117;
            TRUNCATE TABLE mlops_p118;
            TRUNCATE TABLE mlops_p119;
            TRUNCATE TABLE mlops_p120;
            TRUNCATE TABLE mlops_p121;
            TRUNCATE TABLE mlops_p122;
            TRUNCATE TABLE mlops_p123;
            TRUNCATE TABLE mlops_p124;
            TRUNCATE TABLE mlops_p125;
            TRUNCATE TABLE mlops_p126;
            TRUNCATE TABLE mlops_p127;
            TRUNCATE TABLE mlops_p128;
            TRUNCATE TABLE mlops_p129;
            TRUNCATE TABLE mlops_p130;
            TRUNCATE TABLE mlops_p131;
            TRUNCATE TABLE mlops_p132;
            TRUNCATE TABLE mlops_p133;
            TRUNCATE TABLE mlops_p134;
            TRUNCATE TABLE mlops_p135;
            TRUNCATE TABLE mlops_p136;
            TRUNCATE TABLE mlops_p137;
            TRUNCATE TABLE mlops_p138;
            TRUNCATE TABLE mlops_p139;
            TRUNCATE TABLE mlops_p140;
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

            CREATE TABLE IF NOT EXISTS mlops_p3 PARTITION OF mlops
                FOR VALUES FROM (2000) TO (3000);

            CREATE TABLE IF NOT EXISTS mlops_p4 PARTITION OF mlops
                FOR VALUES FROM (3000) TO (4000);

            CREATE TABLE IF NOT EXISTS mlops_p5 PARTITION OF mlops
                FOR VALUES FROM (4000) TO (5000);

            CREATE TABLE IF NOT EXISTS mlops_p6 PARTITION OF mlops
                FOR VALUES FROM (5000) TO (6000);

            CREATE TABLE IF NOT EXISTS mlops_p7 PARTITION OF mlops
                FOR VALUES FROM (6000) TO (7000);

            CREATE TABLE IF NOT EXISTS mlops_p8 PARTITION OF mlops
                FOR VALUES FROM (7000) TO (8000);
            CREATE TABLE IF NOT EXISTS mlops_p9 PARTITION OF mlops
                FOR VALUES FROM (8000) TO (9000);

            CREATE TABLE IF NOT EXISTS mlops_p10 PARTITION OF mlops
                FOR VALUES FROM (9000) TO (10000);

            CREATE TABLE IF NOT EXISTS mlops_p11 PARTITION OF mlops
                FOR VALUES FROM (10000) TO (11000);

            CREATE TABLE IF NOT EXISTS mlops_p12 PARTITION OF mlops
                FOR VALUES FROM (11000) TO (12000);

            CREATE TABLE IF NOT EXISTS mlops_p13 PARTITION OF mlops
                FOR VALUES FROM (12000) TO (13000);

            CREATE TABLE IF NOT EXISTS mlops_p14 PARTITION OF mlops
                FOR VALUES FROM (13000) TO (14000);

            CREATE TABLE IF NOT EXISTS mlops_p15 PARTITION OF mlops
                FOR VALUES FROM (14000) TO (15000);

            CREATE TABLE IF NOT EXISTS mlops_p16 PARTITION OF mlops
                FOR VALUES FROM (15000) TO (16000);

            CREATE TABLE IF NOT EXISTS mlops_p17 PARTITION OF mlops
                FOR VALUES FROM (16000) TO (17000);

            CREATE TABLE IF NOT EXISTS mlops_p18 PARTITION OF mlops
                FOR VALUES FROM (17000) TO (18000);

            CREATE TABLE IF NOT EXISTS mlops_p19 PARTITION OF mlops
                FOR VALUES FROM (18000) TO (19000);

            CREATE TABLE IF NOT EXISTS mlops_p20 PARTITION OF mlops
                FOR VALUES FROM (19000) TO (20000);

            CREATE TABLE IF NOT EXISTS mlops_p21 PARTITION OF mlops
                FOR VALUES FROM (20000) TO (21000);

            CREATE TABLE IF NOT EXISTS mlops_p22 PARTITION OF mlops
                FOR VALUES FROM (21000) TO (22000);

            CREATE TABLE IF NOT EXISTS mlops_p23 PARTITION OF mlops
                FOR VALUES FROM (22000) TO (23000);

            CREATE TABLE IF NOT EXISTS mlops_p24 PARTITION OF mlops
                FOR VALUES FROM (23000) TO (24000);
            CREATE TABLE IF NOT EXISTS mlops_p25 PARTITION OF mlops
                FOR VALUES FROM (24000) TO (25000);

            CREATE TABLE IF NOT EXISTS mlops_p26 PARTITION OF mlops
                FOR VALUES FROM (25000) TO (26000);

            CREATE TABLE IF NOT EXISTS mlops_p27 PARTITION OF mlops
                FOR VALUES FROM (26000) TO (27000);

            CREATE TABLE IF NOT EXISTS mlops_p28 PARTITION OF mlops
                FOR VALUES FROM (27000) TO (28000);

            CREATE TABLE IF NOT EXISTS mlops_p29 PARTITION OF mlops
                FOR VALUES FROM (28000) TO (29000);

            CREATE TABLE IF NOT EXISTS mlops_p30 PARTITION OF mlops
                FOR VALUES FROM (29000) TO (30000);

            CREATE TABLE IF NOT EXISTS mlops_p31 PARTITION OF mlops
                FOR VALUES FROM (30000) TO (31000);

            CREATE TABLE IF NOT EXISTS mlops_p32 PARTITION OF mlops
                FOR VALUES FROM (31000) TO (32000);

            CREATE TABLE IF NOT EXISTS mlops_p33 PARTITION OF mlops
                FOR VALUES FROM (32000) TO (33000);

            CREATE TABLE IF NOT EXISTS mlops_p34 PARTITION OF mlops
                FOR VALUES FROM (33000) TO (34000);

            CREATE TABLE IF NOT EXISTS mlops_p35 PARTITION OF mlops
                FOR VALUES FROM (34000) TO (35000);

            CREATE TABLE IF NOT EXISTS mlops_p36 PARTITION OF mlops
                FOR VALUES FROM (35000) TO (36000);

            CREATE TABLE IF NOT EXISTS mlops_p37 PARTITION OF mlops
                FOR VALUES FROM (36000) TO (37000);

            CREATE TABLE IF NOT EXISTS mlops_p38 PARTITION OF mlops
                FOR VALUES FROM (37000) TO (38000);

            CREATE TABLE IF NOT EXISTS mlops_p39 PARTITION OF mlops
                FOR VALUES FROM (38000) TO (39000);

            CREATE TABLE IF NOT EXISTS mlops_p40 PARTITION OF mlops
                FOR VALUES FROM (39000) TO (40000);

            CREATE TABLE IF NOT EXISTS mlops_p41 PARTITION OF mlops 
                FOR VALUES FROM (40000) TO (41000);

            CREATE TABLE IF NOT EXISTS mlops_p42 PARTITION OF mlops
                FOR VALUES FROM (41000) TO (42000);

            CREATE TABLE IF NOT EXISTS mlops_p43 PARTITION OF mlops
                FOR VALUES FROM (42000) TO (43000);

            CREATE TABLE IF NOT EXISTS mlops_p44 PARTITION OF mlops
                FOR VALUES FROM (43000) TO (44000);

            CREATE TABLE IF NOT EXISTS mlops_p45 PARTITION OF mlops
                FOR VALUES FROM (44000) TO (45000);

            CREATE TABLE IF NOT EXISTS mlops_p46 PARTITION OF mlops
                FOR VALUES FROM (45000) TO (46000);

            CREATE TABLE IF NOT EXISTS mlops_p47 PARTITION OF mlops
                FOR VALUES FROM (46000) TO (47000);

            CREATE TABLE IF NOT EXISTS mlops_p48 PARTITION OF mlops
                FOR VALUES FROM (47000) TO (48000);

            CREATE TABLE IF NOT EXISTS mlops_p49 PARTITION OF mlops
                FOR VALUES FROM (48000) TO (49000);

            CREATE TABLE IF NOT EXISTS mlops_p50 PARTITION OF mlops
                FOR VALUES FROM (49000) TO (50000);

            CREATE TABLE IF NOT EXISTS mlops_p51 PARTITION OF mlops
                FOR VALUES FROM (50000) TO (51000);

            CREATE TABLE IF NOT EXISTS mlops_p52 PARTITION OF mlops
                FOR VALUES FROM (51000) TO (52000);

            CREATE TABLE IF NOT EXISTS mlops_p53 PARTITION OF mlops
                FOR VALUES FROM (52000) TO (53000);

            CREATE TABLE IF NOT EXISTS mlops_p54 PARTITION OF mlops
                FOR VALUES FROM (53000) TO (54000);

            CREATE TABLE IF NOT EXISTS mlops_p55 PARTITION OF mlops
                FOR VALUES FROM (54000) TO (55000);

            CREATE TABLE IF NOT EXISTS mlops_p56 PARTITION OF mlops
                FOR VALUES FROM (55000) TO (56000);

            CREATE TABLE IF NOT EXISTS mlops_p57 PARTITION OF mlops
                FOR VALUES FROM (56000) TO (57000);

            CREATE TABLE IF NOT EXISTS mlops_p58 PARTITION OF mlops
                FOR VALUES FROM (57000) TO (58000);

            CREATE TABLE IF NOT EXISTS mlops_p59 PARTITION OF mlops
                FOR VALUES FROM (58000) TO (59000);

            CREATE TABLE IF NOT EXISTS mlops_p60 PARTITION OF mlops
                FOR VALUES FROM (59000) TO (60000);

            CREATE TABLE IF NOT EXISTS mlops_p61 PARTITION OF mlops
                FOR VALUES FROM (60000) TO (61000);

            CREATE TABLE IF NOT EXISTS mlops_p62 PARTITION OF mlops
                FOR VALUES FROM (61000) TO (62000);

            CREATE TABLE IF NOT EXISTS mlops_p63 PARTITION OF mlops
                FOR VALUES FROM (62000) TO (63000);

            CREATE TABLE IF NOT EXISTS mlops_p64 PARTITION OF mlops
                FOR VALUES FROM (63000) TO (64000);

            CREATE TABLE IF NOT EXISTS mlops_p65 PARTITION OF mlops
                FOR VALUES FROM (64000) TO (65000);

            CREATE TABLE IF NOT EXISTS mlops_p66 PARTITION OF mlops
                FOR VALUES FROM (65000) TO (66000);

            CREATE TABLE IF NOT EXISTS mlops_p67 PARTITION OF mlops
                FOR VALUES FROM (66000) TO (67000);

            CREATE TABLE IF NOT EXISTS mlops_p68 PARTITION OF mlops
                FOR VALUES FROM (67000) TO (68000);

            CREATE TABLE IF NOT EXISTS mlops_p69 PARTITION OF mlops
                FOR VALUES FROM (68000) TO (69000);

            CREATE TABLE IF NOT EXISTS mlops_p70 PARTITION OF mlops
                FOR VALUES FROM (69000) TO (70000);

            CREATE TABLE IF NOT EXISTS mlops_p71 PARTITION OF mlops
                FOR VALUES FROM (70000) TO (71000);

            CREATE TABLE IF NOT EXISTS mlops_p72 PARTITION OF mlops
                FOR VALUES FROM (71000) TO (72000);

            CREATE TABLE IF NOT EXISTS mlops_p73 PARTITION OF mlops
                FOR VALUES FROM (72000) TO (73000);

            CREATE TABLE IF NOT EXISTS mlops_p74 PARTITION OF mlops
                FOR VALUES FROM (73000) TO (74000);

            CREATE TABLE IF NOT EXISTS mlops_p75 PARTITION OF mlops
                FOR VALUES FROM (74000) TO (75000);

            CREATE TABLE IF NOT EXISTS mlops_p76 PARTITION OF mlops
                FOR VALUES FROM (75000) TO (76000);

            CREATE TABLE IF NOT EXISTS mlops_p77 PARTITION OF mlops
                FOR VALUES FROM (76000) TO (77000);

            CREATE TABLE IF NOT EXISTS mlops_p78 PARTITION OF mlops
                FOR VALUES FROM (77000) TO (78000);

            CREATE TABLE IF NOT EXISTS mlops_p79 PARTITION OF mlops
                FOR VALUES FROM (78000) TO (79000);

            CREATE TABLE IF NOT EXISTS mlops_p80 PARTITION OF mlops
                FOR VALUES FROM (79000) TO (80000);

            CREATE TABLE IF NOT EXISTS mlops_p81 PARTITION OF mlops
                FOR VALUES FROM (80000) TO (81000);

            CREATE TABLE IF NOT EXISTS mlops_p82 PARTITION OF mlops
                FOR VALUES FROM (81000) TO (82000);

            CREATE TABLE IF NOT EXISTS mlops_p83 PARTITION OF mlops
                FOR VALUES FROM (82000) TO (83000);

            CREATE TABLE IF NOT EXISTS mlops_p84 PARTITION OF mlops
                FOR VALUES FROM (83000) TO (84000);
            
            CREATE TABLE IF NOT EXISTS mlops_p85 PARTITION OF mlops
                FOR VALUES FROM (84000) TO (85000);

            CREATE TABLE IF NOT EXISTS mlops_p86 PARTITION OF mlops
                FOR VALUES FROM (85000) TO (86000);

            CREATE TABLE IF NOT EXISTS mlops_p87 PARTITION OF mlops
                FOR VALUES FROM (86000) TO (87000);

            CREATE TABLE IF NOT EXISTS mlops_p88 PARTITION OF mlops
                FOR VALUES FROM (87000) TO (88000);

            CREATE TABLE IF NOT EXISTS mlops_p89 PARTITION OF mlops
                FOR VALUES FROM (88000) TO (89000);

            CREATE TABLE IF NOT EXISTS mlops_p90 PARTITION OF mlops
                FOR VALUES FROM (89000) TO (90000);

            CREATE TABLE IF NOT EXISTS mlops_p91 PARTITION OF mlops
                FOR VALUES FROM (90000) TO (91000);

            CREATE TABLE IF NOT EXISTS mlops_p92 PARTITION OF mlops
                FOR VALUES FROM (91000) TO (92000);

            CREATE TABLE IF NOT EXISTS mlops_p93 PARTITION OF mlops
                FOR VALUES FROM (92000) TO (93000);

            CREATE TABLE IF NOT EXISTS mlops_p94 PARTITION OF mlops
                FOR VALUES FROM (93000) TO (94000);

            CREATE TABLE IF NOT EXISTS mlops_p95 PARTITION OF mlops
                FOR VALUES FROM (94000) TO (95000);

            CREATE TABLE IF NOT EXISTS mlops_p96 PARTITION OF mlops
                FOR VALUES FROM (95000) TO (96000);

            CREATE TABLE IF NOT EXISTS mlops_p97 PARTITION OF mlops
                FOR VALUES FROM (96000) TO (97000);

            CREATE TABLE IF NOT EXISTS mlops_p98 PARTITION OF mlops
                FOR VALUES FROM (97000) TO (98000);

            CREATE TABLE IF NOT EXISTS mlops_p99 PARTITION OF mlops
                FOR VALUES FROM (98000) TO (99000);

            CREATE TABLE IF NOT EXISTS mlops_p100 PARTITION OF mlops
                FOR VALUES FROM (99000) TO (100000);

            CREATE TABLE IF NOT EXISTS mlops_p101 PARTITION OF mlops
                FOR VALUES FROM (100000) TO (101000);

            CREATE TABLE IF NOT EXISTS mlops_p102 PARTITION OF mlops
                FOR VALUES FROM (101000) TO (102000);

            CREATE TABLE IF NOT EXISTS mlops_p103 PARTITION OF mlops
                FOR VALUES FROM (102000) TO (103000);

            CREATE TABLE IF NOT EXISTS mlops_p104 PARTITION OF mlops
                FOR VALUES FROM (103000) TO (104000);

            CREATE TABLE IF NOT EXISTS mlops_p105 PARTITION OF mlops
                FOR VALUES FROM (104000) TO (105000);

            CREATE TABLE IF NOT EXISTS mlops_p106 PARTITION OF mlops
                FOR VALUES FROM (105000) TO (106000);

            CREATE TABLE IF NOT EXISTS mlops_p107 PARTITION OF mlops
                FOR VALUES FROM (106000) TO (107000);

            CREATE TABLE IF NOT EXISTS mlops_p108 PARTITION OF mlops
                FOR VALUES FROM (107000) TO (108000);

            CREATE TABLE IF NOT EXISTS mlops_p109 PARTITION OF mlops
                FOR VALUES FROM (108000) TO (109000);

            CREATE TABLE IF NOT EXISTS mlops_p110 PARTITION OF mlops
                FOR VALUES FROM (109000) TO (110000);

            CREATE TABLE IF NOT EXISTS mlops_p111 PARTITION OF mlops
                FOR VALUES FROM (110000) TO (111000);

            CREATE TABLE IF NOT EXISTS mlops_p112 PARTITION OF mlops
                FOR VALUES FROM (111000) TO (112000);

            CREATE TABLE IF NOT EXISTS mlops_p113 PARTITION OF mlops
                FOR VALUES FROM (112000) TO (113000);

            CREATE TABLE IF NOT EXISTS mlops_p114 PARTITION OF mlops
                FOR VALUES FROM (113000) TO (114000);

            CREATE TABLE IF NOT EXISTS mlops_p115 PARTITION OF mlops
                FOR VALUES FROM (114000) TO (115000);

            CREATE TABLE IF NOT EXISTS mlops_p116 PARTITION OF mlops
                FOR VALUES FROM (115000) TO (116000);

            CREATE TABLE IF NOT EXISTS mlops_p117 PARTITION OF mlops
                FOR VALUES FROM (116000) TO (117000);

            CREATE TABLE IF NOT EXISTS mlops_p118 PARTITION OF mlops
                FOR VALUES FROM (117000) TO (118000);

            CREATE TABLE IF NOT EXISTS mlops_p119 PARTITION OF mlops
                FOR VALUES FROM (118000) TO (119000);

            CREATE TABLE IF NOT EXISTS mlops_p120 PARTITION OF mlops
                FOR VALUES FROM (119000) TO (120000);

            CREATE TABLE IF NOT EXISTS mlops_p121 PARTITION OF mlops
                FOR VALUES FROM (120000) TO (121000);

            CREATE TABLE IF NOT EXISTS mlops_p122 PARTITION OF mlops
                FOR VALUES FROM (121000) TO (122000);

            CREATE TABLE IF NOT EXISTS mlops_p123 PARTITION OF mlops
                FOR VALUES FROM (122000) TO (123000);

            CREATE TABLE IF NOT EXISTS mlops_p124 PARTITION OF mlops
                FOR VALUES FROM (123000) TO (124000);

            CREATE TABLE IF NOT EXISTS mlops_p125 PARTITION OF mlops
                FOR VALUES FROM (124000) TO (125000);

            CREATE TABLE IF NOT EXISTS mlops_p126 PARTITION OF mlops
                FOR VALUES FROM (125000) TO (126000);

            CREATE TABLE IF NOT EXISTS mlops_p127 PARTITION OF mlops
                FOR VALUES FROM (126000) TO (127000);

            CREATE TABLE IF NOT EXISTS mlops_p128 PARTITION OF mlops
                FOR VALUES FROM (127000) TO (128000);

            CREATE TABLE IF NOT EXISTS mlops_p129 PARTITION OF mlops
                FOR VALUES FROM (128000) TO (129000);

            CREATE TABLE IF NOT EXISTS mlops_p130 PARTITION OF mlops
                FOR VALUES FROM (129000) TO (130000);

            CREATE TABLE IF NOT EXISTS mlops_p131 PARTITION OF mlops
                FOR VALUES FROM (130000) TO (131000);

            CREATE TABLE IF NOT EXISTS mlops_p132 PARTITION OF mlops
                FOR VALUES FROM (131000) TO (132000);

            CREATE TABLE IF NOT EXISTS mlops_p133 PARTITION OF mlops
                FOR VALUES FROM (132000) TO (133000);

            CREATE TABLE IF NOT EXISTS mlops_p134 PARTITION OF mlops
                FOR VALUES FROM (133000) TO (134000);

            CREATE TABLE IF NOT EXISTS mlops_p135 PARTITION OF mlops
                FOR VALUES FROM (134000) TO (135000);

            CREATE TABLE IF NOT EXISTS mlops_p136 PARTITION OF mlops
                FOR VALUES FROM (135000) TO (136000);

            CREATE TABLE IF NOT EXISTS mlops_p137 PARTITION OF mlops
                FOR VALUES FROM (136000) TO (137000);

            CREATE TABLE IF NOT EXISTS mlops_p138 PARTITION OF mlops
                FOR VALUES FROM (137000) TO (138000);

            CREATE TABLE IF NOT EXISTS mlops_p139 PARTITION OF mlops
                FOR VALUES FROM (138000) TO (139000);

            CREATE TABLE IF NOT EXISTS mlops_p140 PARTITION OF mlops
                FOR VALUES FROM (139000) TO (140000);

                   '''

# create_table_query = '''
#            drop table document22 cascade;
#            CREATE TABLE IF NOT EXISTS document22 (
#                file_name TEXT,
#                doc_type VARCHAR,
#                id SERIAL PRIMARY KEY
#            );
#            drop table chunks22 cascade;
#            CREATE TABLE IF NOT EXISTS chunks22 (   
#                chunk TEXT,
#                embedding BYTEA, 
#                document_id INT,
#                id SERIAL PRIMARY KEY,
#                FOREIGN KEY (document_id) REFERENCES document22(id)
#            );
#            drop table metadata22 cascade;
#            CREATE TABLE IF NOT EXISTS metadata22 (
#                meta_key TEXT,
#                value TEXT,
#                document_id INT,
#                id SERIAL PRIMARY KEY,
#                FOREIGN KEY (document_id) REFERENCES document22(id)
#            );
#                    '''
# create_table_query = '''
#             CREATE EXTENSION vector;
#             drop table embeddings22 cascade;
#             CREATE TABLE IF NOT EXISTS embeddings22 (
#             id bigserial primary key, 
#             embedding vector(1536)
#             );
#                     '''


try:
    cursor.execute(create_table_query)
    connection.commit()
    print("mlops tables created successfully!")
except (Exception, psycopg2.Error) as error:
    print("Error creating table:", error)

# Validate table creation
try:
    connection = psycopg2.connect(
#        user="vectorsmith",
        user="postgres",
        password="supportvectors.123",
        host="localhost",
        port="5432",
        database="vectordb"
    )
    cursor = connection.cursor()
    cursor.execute("SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'mlops_p1');")
    table_exists = cursor.fetchone()[0]
    if table_exists:
        print("Table 'mlops' exists.")
    else:
        print("Table 'mlops' does not exist.")
except (Exception, psycopg2.Error) as error:
    print("Error checking table existence:", error)
finally:
    if connection:
        cursor.close()

# Validate table creation
try:
    connection = psycopg2.connect(
#        user="vectorsmith",
        user="postgres",
        password="supportvectors.123",
        host="localhost",
        port="5432",
        database="vectordb"
    )
    cursor = connection.cursor()
    #cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'document22');")
    cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'mlops');")
    table_exists = cursor.fetchone()[0]
    if table_exists:
        print("Table 'mlops' exists.")
    else:
        print("Table 'mlops' does not exist.")
except (Exception, psycopg2.Error) as error:
    print("Error checking table existence:", error)

# Validate table creation
try:
    connection = psycopg2.connect(
#        user="vectorsmith",
        user="postgres",
        password="supportvectors.123",
        host="localhost",
        port="5432",
        database="vectordb"
    )
    cursor = connection.cursor()
    #cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'metadata');")
    #cursor.execute("SELECT chunk FROM chunks WHERE id = '67';")
#    cursor.execute("SELECT chunk, id FROM chunks22;")
    all_results = cursor.fetchall()
    print("all results", all_results)
    table_exists = cursor.fetchone()[0]
    if table_exists:
        print("Table 'metadata' exists.")
    else:
        print("Table 'metadata' does not exist.")
except (Exception, psycopg2.Error) as error:
    print("Error checking table existence:", error)
