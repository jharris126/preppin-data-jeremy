create table input_prepped as
select
    string_split(wt."﻿OrderID", '-')[0] as "Store",
    string_split(wt."﻿OrderID", '-')[1] as "OrderID",
    wt."Customer",
    wt."Order Date",
    wt."Category",
    wt."Sub-Category",
    wt."Product Name",
    cast(replace(wt."Unit Price", '£', '') as decimal(12, 2)) as "Unit Price",
    wt."Quantity",
    case
        when "Return State" = 'Return Processed' then 1
        else 0
    end as "Returned"
from "input" as wt;

create table store_dim as
select
    row_number() over(order by min("Order Date"), "Store") as "StoreID",
    "Store",
    min("Order Date") as "First Order"
from input_prepped
group by "Store";

create table product_dim as
select
    row_number() over(order by min("Order Date"), "Product Name") as "ProductID",
    "Category",
    "Sub-Category",
    "Product Name",
    "Unit Price",
    min("Order Date") as "First Sold"
from input_prepped
group by
    "Category",
    "Sub-Category",
    "Product Name",
    "Unit Price";

create table customer_dim as
select
    row_number() over(order by min("Order Date"), "Customer") as "CustomerID",
    "Customer",
    round(cast(sum("Returned") as decimal)/sum(1), 2) as "Return %",
    count(distinct "OrderID") as "Number of Orders",
    min("Order Date") as "First Order"
from input_prepped
group by "Customer";

create table fact_table as
select
    sd."StoreID",
    cd."CustomerID",
    ip."OrderID",
    ip."Order Date",
    pd."ProductID",
    ip."Returned",
    ip."Quantity",
    ip."Unit Price" * "Quantity" as "Sales"
from input_prepped as ip
left join store_dim as sd
    on ip."Store" = sd."Store"
left join customer_dim as cd
    on ip."Customer" = cd."Customer"
left join product_dim as pd
    on ip."Product Name" = pd."Product Name";

drop table "input";
drop table input_prepped;

PRAGMA show_tables;
