import os
from tableauhyperapi import HyperProcess, Connection, CreateMode, TableDefinition, SqlType, escape_string_literal,\
    Telemetry


# start hyper process and open connection to database
with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(
        endpoint=hyper.endpoint, database='2022-2-output.hyper', create_mode=CreateMode.CREATE_AND_REPLACE
    ) as connection:

        # create table definition object
        table_def = TableDefinition('input', [
            TableDefinition.Column('id', SqlType.int()),
            TableDefinition.Column('pupil first name', SqlType.text()),
            TableDefinition.Column('pupil last name', SqlType.text()),
            TableDefinition.Column('gender', SqlType.text()),
            TableDefinition.Column('Date of Birth', SqlType.text()),
            TableDefinition.Column('Parental Contact Name_1', SqlType.text()),
            TableDefinition.Column('Parental Contact Name_2', SqlType.text()),
            TableDefinition.Column('Preferred Contact Employer', SqlType.text()),
            TableDefinition.Column('Parental Contact', SqlType.int())
        ])

        # create hyper table from table definition
        connection.catalog.create_table(table_def)

        # copy input csv to hyper table named "input"
        copy_sql = (
            f"COPY {table_def.table_name} "
            f"FROM {escape_string_literal(os.path.join('..', 'PD 2022 Wk 2 Input.csv'))} "
            f"WITH (FORMAT CSV, NULL 'NULL', delimiter ',', header)"
        )
        connection.execute_command(copy_sql)

        # create output table using create table as plus sql data prep logic from prep.sql to shape data
        sql = 'create table "output" as ' + open('prep.sql', 'r').read()
        connection.execute_command(sql)
