import duckdb
import os


# table to be created in duckdb from csv
dataset = {'file': os.path.join('..', 'PD 2022 Wk 2 Input.csv'), 'table': 'input'}

# create a duckdb connection object from which all duckdb queries and commands will be executed
conn = duckdb.connect()

# generate sql to load the file into a table and duckdb command to execute it
cta_sql = f"create table {dataset['table']} as select * from read_csv_auto('{dataset['file']}');"
conn.execute(cta_sql)

# sql to perform the data prep, sql stored in separate prep.sql file in this directory for improved clarity
sql = open('prep.sql', 'r').read()

# print list of tuples for testing prep.sql output
# print(conn.execute(sql).fetchall())

# define output file name/path, sql to execute and export, and duckdb command to execute the sql
outfile = '2022-2-output.csv'
export_sql = f"copy ({sql}) to '{outfile}' with (header 1, delimiter ',');"
conn.execute(export_sql)
