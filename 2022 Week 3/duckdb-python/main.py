import duckdb
import os


# loads csv data files form datasets variable and inserts into a table named whatever string is in dataset table key
def load_to_table():
    # sql to load the file into a table and duckdb command to execute it
    for dataset in datasets:
        cta_sql = f"create table {dataset['table']} as select * from read_csv_auto('{dataset['file']}');"
        conn.execute(cta_sql)


def transform_and_write():
    # sql to perform the data prep laid out in the challenge
    sql = '''
        with grades_upv as (
            select
                "Student ID",
                unnest(array['Maths', 'English', 'Spanish', 'Science', 'Art', 'History', 'Geography']) as "Subject",
                unnest(array["Maths", "English", "Spanish", "Science", "Art", "History", "Geography"]) as "Score"
            from grades
        ),
        
        grades_summary as (
            select
                sum (
                    case
                        when "Score" >= 75 then 1
                        else 0
                    end
                ) as "Passed Subjects",
                round(avg("Score"), 1) as "Student's Avg Score",
                "Student ID"
            from grades_upv
            group by "Student ID"
        )
        
        select
            gs."Passed Subjects",
            gs."Student's Avg Score",
            gs."Student ID",
            r.Gender as "Gender"
        from roster as r
        inner join grades_summary as gs
            on r."id" = gs."Student ID"
    '''

    # print tuple for testing
    # print(conn.execute(sql).fetchall())

    # define output file name/path, sql to execute and export, and duckdb command to execute the sql
    outfile = '2022-3-output.csv'
    export_sql = f"copy ({sql}) to '{outfile}' with (header 1, delimiter ',');"
    conn.execute(export_sql)


# main function, best practice in python for creating a starting point and context for beginning code execution
if __name__ == '__main__':
    # tables to be created in duckdb from csv
    datasets = [
        {
            'file': os.path.join('..', 'PD 2022 Wk 3 Input.csv'),
            'table': 'roster'
        },
        {
            'file': os.path.join('..', 'PD 2022 WK 3 Grades.csv'),
            'table': 'grades'
        }
    ]

    # create a duckdb connection object from which all duckdb queries and commands will be executed
    conn = duckdb.connect()

    # invoke function to load input csv data to a duckdb table
    load_to_table()

    # invoke function to transform (prep) the data and export as output csv
    transform_and_write()
