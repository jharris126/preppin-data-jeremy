from tableauhyperapi import SqlType, HyperProcess, Telemetry, Connection, CreateMode, TableDefinition,\
    escape_string_literal


# read input file in as arrow table
input_file = '../PD 2022 Wk 1 Input - Input.csv'

with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(
            endpoint=hyper.endpoint, database='output.hyper', create_mode=CreateMode.CREATE_AND_REPLACE
    ) as connection:

        table_definition = TableDefinition('input', [
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

        connection.catalog.create_table(table_definition=table_definition)

        copy_command = (
            f"COPY {table_definition.table_name} "
            f"FROM {escape_string_literal(input_file)} "
            f"WITH (FORMAT CSV, NULL 'NULL', delimiter ',', header)"
        )

        connection.execute_command(copy_command)

        # use hyper sql to prep the data
        sql = '''
            create table "output" as
            select
                cast(
                    ceil(
                        (
                            (DATE_PART('year', '2014-09-01'::date) - DATE_PART('year', "Date of Birth"::date)) * 12 +
                            (DATE_PART('month', '2014-09-01'::date) - DATE_PART('month', "Date of Birth"::date))
                        ) / 12.0
                    ) + 1 as int
                ) as "Academic Year",
                concat("pupil last name", ', ', "pupil first name") as "Pupils Name",
                case
                    when "Parental Contact" = 1
                        then concat("pupil last name", ', ', "Parental Contact Name_1")
                    else
                        concat("pupil last name", ', ', "Parental Contact Name_2")
                end as "Parental Contact Full Name",
                case
                    when "Parental Contact" = 1 then
                        concat(
                            "Parental Contact Name_1", '.', "pupil last name", '@', "Preferred Contact Employer", '.com'
                        )
                    else
                        concat(
                            "Parental Contact Name_2", '.', "pupil last name", '@', "Preferred Contact Employer", '.com'
                        )
                end as "Parental Contact Email Address"
            from "input"
            '''

        connection.execute_command(sql)
        connection.execute_command('DROP TABLE "input"')
