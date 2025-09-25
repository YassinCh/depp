-- Simple SQL model that creates base data
{{ config(materialized='table') }}

select
    1 as id,
    'Product A' as product_name,
    100.50 as price,
    10 as quantity
union all
select
    2 as id,
    'Product B' as product_name,
    75.25 as price,
    5 as quantity
union all
select
    3 as id,
    'Product C' as product_name,
    200.00 as price,
    8 as quantity