{{config(materialized='incremental', unique_key='account_id')}}

with temp as (
    select * from {{ref('bronze_acc')}}
),

unpacked as (
    select * from temp
)

select * from unpacked