{{config(materialized='table')}}

with temp as (
    select * from {{ref('silver_acc')}}
),

unpacked as (
    select * from temp
)

select * from unpacked