from os import path
from duckdb import connect


# table to be created in duckdb from csv
dataset = {'file': path.join('..', '2021W51 Input.csv'), 'table': 'input'}

# create a duckdb connection object from which all duckdb queries and commands will be executed
conn = connect()

# generate sql to load the file into a table and duckdb command to execute it
cta_sql = f"create table {dataset['table']} as select * from read_csv_auto('{dataset['file']}');"
conn.execute(cta_sql)

# sql to perform the data prep, sql stored in separate prep.sql file in this directory for improved clarity
sql = open('prep.sql', 'r').read()

# perform transformations that materialize tables and get list of table names
tables_ls = conn.execute(sql).fetchall()

# define output file names/paths, sql to execute and export, and duckdb commands to execute the sql

for tab in tables_ls:
    name = tab[0]
    outfile = name + '.csv'
    export_sql = f"copy {name} to '{outfile}' with (header 1, delimiter ',');"
    conn.execute(export_sql)
