-- unpivot subjects from columns to rows (from 1 row per student to 1 row per student per subject)
--with grades_upv as (
    select
        "Student ID",
        unnest(array['Maths', 'English', 'Spanish', 'Science', 'Art', 'History', 'Geography']) as "Subject",
        unnest(array["Maths", "English", "Spanish", "Science", "Art", "History", "Geography"]) as "Score"
    from grades
    where "Student ID" = 1
--),

-- calculate passing students per subject then count passing subjects and average scores per student
/*grades_summary as (
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

-- join grades summary logic to class roster table on student id to get gender demographics
select
    gs."Passed Subjects",
    gs."Student's Avg Score",
    gs."Student ID",
    r.Gender as "Gender"
from roster as r
inner join grades_summary as gs
    on r."id" = gs."Student ID"*/
