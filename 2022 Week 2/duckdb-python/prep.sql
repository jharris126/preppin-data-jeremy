with bday as (
    select
        "id",
        concat("pupil first name", ' ', "pupil last name")  as "Pupil Name",
        "Date of Birth",
        "Date of Birth" + INTERVAL (2022 - date_part('year', "Date of Birth")) YEAR as "This Year's Birthday",
        monthname("Date of Birth") as "Month"
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
            when dayname("This Year's Birthday") in ('Saturday', 'Sunday') then 'Friday'
            else dayname("This Year's Birthday")
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