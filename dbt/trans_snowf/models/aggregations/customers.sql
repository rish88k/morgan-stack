{{ config(materialized='table') }}

with temp as (
    select * from {{ ref('bronze_cust') }}
),

unpacked as (
    select
        count(customer_id) as no_of_customers,
        country
    from temp
    group by country
    order by no_of_customers desc
)

select * from unpacked
