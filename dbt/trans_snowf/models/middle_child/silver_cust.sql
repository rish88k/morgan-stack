{{config(materialized='incremental', unique_key='customer_id')}}

with temp as (
    select * from {{ref('bronze_cust')}}
),

unpacked as (
    select * from temp
)

select * from unpacked