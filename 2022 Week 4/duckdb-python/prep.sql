-- unpivot weekday travel from columns to rows, unnecessary join with week 1 class roster because it says to...
with upv as (
    select
        r."id" as "Student ID",
        unnest(array['M', 'Tu', 'W', 'Th', 'F']) as "Weekday",
        unnest(array["M", "Tu", "W", "Th", "F"]) as "Method of Travel"
    from roster as r
    inner join travel as t
        on r."id" = t."Student ID"
),

-- fix/group misspelled words
cleansed as (
    select
        "Student ID",
        "Weekday",
        case
            when "Method of Travel" = 'Bycycle' then 'Bicycle'
            when "Method of Travel" = 'Carr' then 'Car'
            when "Method of Travel" in ('Walkk', 'Wallk', 'WAlk', 'Waalk') then 'Walk'
            when "Method of Travel" in ('Scootr', 'Scoter') then 'Scooter'
            when "Method of Travel" = 'Helicopeter' then 'Helicopter'
            else "Method of Travel"
        end as "Method of Travel"
    from upv
)

-- categorize sustainability, count trips per day/method, cross join student count, divide method count by trips
select
    case
        when "Method of Travel" in ('Car', 'Aeroplane', 'Helicopter', 'Van') then 'Non-Sustainable'
        else 'Sustainable'
    end as "Sustainable?",
    "Method of Travel",
    "Weekday",
    sum(1) as "Number of Trips",
    n."Trips per Day",
    round(cast(sum(1) as numeric)/n."Trips per Day", 2) as "% of trips per day"
from cleansed as c
cross join (select count(distinct "Student ID") as "Trips per Day" from upv) as n
group by "Weekday", "Method of Travel", n."Trips per Day"
