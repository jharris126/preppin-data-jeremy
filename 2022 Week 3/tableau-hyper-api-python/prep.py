import os
from tableauhyperapi import HyperProcess, Connection, CreateMode, TableDefinition, SqlType, escape_string_literal,\
    Telemetry


# start hyper process and open connection to database
with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(
        endpoint=hyper.endpoint, database='2022-3-output.hyper', create_mode=CreateMode.CREATE_AND_REPLACE
    ) as connection:

        # create roster table definition object
        roster_def = TableDefinition('roster', [
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

        # create grades table definition object
        grades_def = TableDefinition('grades', [
            TableDefinition.Column('Student ID', SqlType.int()),
            TableDefinition.Column('Maths', SqlType.int()),
            TableDefinition.Column('English', SqlType.int()),
            TableDefinition.Column('Spanish', SqlType.int()),
            TableDefinition.Column('Science', SqlType.int()),
            TableDefinition.Column('Art', SqlType.int()),
            TableDefinition.Column('History', SqlType.int()),
            TableDefinition.Column('Geography', SqlType.int())
        ])

        # create hyper tables from table definitions
        connection.catalog.create_table(roster_def)
        connection.catalog.create_table(grades_def)

        # copy class roster csv to roster table sql
        copy_roster = (
            f"COPY {roster_def.table_name} "
            f"FROM {escape_string_literal(os.path.join('..', 'PD 2022 Wk 3 Input.csv'))} "
            f"WITH (FORMAT CSV, NULL 'NULL', delimiter ',', header)"
        )

        # copy grades csv to grades table sql
        copy_grades = (
            f"COPY {grades_def.table_name} "
            f"FROM {escape_string_literal(os.path.join('..', 'PD 2022 WK 3 Grades.csv'))} "
            f"WITH (FORMAT CSV, NULL 'NULL', delimiter ',', header)"
        )

        # execute both copy sql statements
        connection.execute_command(copy_roster)
        connection.execute_command(copy_grades)

        # create output table using create table as plus sql data prep logic from prep.sql to shape data
        sql = 'create table "output" as ' + open('prep.sql', 'r').read()
        connection.execute_command(sql)
