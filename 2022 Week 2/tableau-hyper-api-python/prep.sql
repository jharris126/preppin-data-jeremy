with bday as (
    select
        "id",
        concat("pupil first name", ' ', "pupil last name")  as "Pupil Name",
        cast("Date of Birth" as date) as "Date of Birth",
        make_date(
            2022,
            extract(MONTH FROM cast("Date of Birth" as date)),
            extract(DAY FROM cast("Date of Birth" as date))
        ) as "This Year's Birthday",
        to_char(cast("Date of Birth" as date), 'Month') as "Month"
    from "input"
),

cake as (
    select
        "id",
        "Pupil Name",
        "Date of Birth",
        "This Year's Birthday",
        "Month",
        case
            when to_char("This Year's Birthday", 'Day') in ('Saturday', 'Sunday') then 'Friday'
            else to_char("This Year's Birthday", 'Day')
        end as "Cake Needed On"
    from bday
)

select
    "Pupil Name",
    "Date of Birth",
    "This Year's Birthday",
    "Month",
    "Cake Needed On",
    sum(1) over(partition by "Month", "Cake Needed On") as "BDs per Weekday and Month"
from cake
order by "id"