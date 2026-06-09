{{config(materialized='table')}}

with temp as (
    select * from {{ref('silver_trans')}}
),

unpacked as (
    select * from temp
)

select * from unpacked