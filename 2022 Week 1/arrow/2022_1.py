from pyarrow import csv
import duckdb

# read input file in as arrow table
input_file = '../PD 2022 Wk 1 Input - Input.csv'
source_arrow_table = csv.read_csv(input_file)

# use duckdb to query arrow table with sql and store in new table
con = duckdb.connect()
arrow_table = con.execute(
    '''
    select
        cast(
            ceil(date_diff('month', strptime("Date of Birth", '%m/%d/%Y'), cast('2014-09-01' as date))/12.0) + 1 as int
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
                concat("Parental Contact Name_1", '.', "pupil last name", '@', "Preferred Contact Employer", '.com')
            else
                concat("Parental Contact Name_2", '.', "pupil last name", '@', "Preferred Contact Employer", '.com')
        end as "Parental Contact Email Address"
    from source_arrow_table
    '''
).fetch_arrow_table()

# output new arrow table to csv
csv.write_csv(arrow_table, 'output.csv')
