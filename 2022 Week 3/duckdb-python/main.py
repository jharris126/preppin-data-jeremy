import duckdb
import os


# tables to be created in duckdb from csv
datasets = [
    {'file': os.path.join('..', 'PD 2022 Wk 3 Input.csv'), 'table': 'roster'},
    {'file': os.path.join('..', 'PD 2022 WK 3 Grades.csv'), 'table': 'grades'}
]

# create a duckdb connection object from which all duckdb queries and commands will be executed
conn = duckdb.connect()

# generate sql to load the files into tables and duckdb command to execute it
for dataset in datasets:
    cta_sql = f"create table {dataset['table']} as select * from read_csv_auto('{dataset['file']}');"
    conn.execute(cta_sql)

# sql to perform the data prep, sql now stored in separate prep.sql file in this directory for improved clarity
sql = open('prep.sql', 'r').read()

# print list of tuples for testing prep.sql output
# print(conn.execute(sql).fetchall())

# define output file name/path, sql to execute and export, and duckdb command to execute the sql
outfile = '2022-3-output.csv'
export_sql = f"copy ({sql}) to '{outfile}' with (header 1, delimiter ',');"
conn.execute(export_sql)
