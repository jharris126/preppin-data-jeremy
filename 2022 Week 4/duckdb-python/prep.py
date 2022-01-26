import duckdb
import os

# tables to be created in duckdb from csv
datasets = [
    {'file': os.path.join('..', 'PD 2021 WK 1 to 4 ideas - Preferences of Travel.csv'), 'table': 'travel'},
    {'file': os.path.join('..', 'PD 2022 Wk 1 Input - Input.csv'), 'table': 'roster'}
]

# create a duckdb connection object from which all duckdb queries and commands will be executed
conn = duckdb.connect()

# generate sql to load the files into tables and duckdb command to execute it
for dataset in datasets:
    cta_sql = f"create table {dataset['table']} as select * from read_csv_auto('{dataset['file']}');"
    conn.execute(cta_sql)

# sql to perform the data prep, sql stored in separate prep.sql file in this directory for clarity
sql = open('prep.sql', 'r').read()

# print list of tuples for testing prep.sql output
# print(conn.execute(sql).fetchone())

# define output file name/path, sql to execute and export, and duckdb command to execute the sql
outfile = '2022-4-output.csv'
export_sql = f"copy ({sql}) to '{outfile}' with (header 1, delimiter ',');"
conn.execute(export_sql)
