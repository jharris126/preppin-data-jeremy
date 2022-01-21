select
    c."Complaint",
    (
        select count(1)
        from "complaints" cc
        where cc."Name" = c."Name"
    ) as "Complaints per Person",
    coalesce(lower(string_agg(d."Keyword", ',')), 'other') as "Complaint causes",
    coalesce(d."Department", 'Unknown') as "Department",
    c."Name"
from "complaints" as c
left join "department_responsbile" as d
    on regexp_matches(lower(c."Complaint"), lower(d."Keyword"))
group by
    c."Complaint",
    c."Name",
    d."Department"
