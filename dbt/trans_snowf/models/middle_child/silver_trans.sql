{{config(materialized='incremental', unique_key='transaction_id')}}

with temp as (
    select * from {{ref('bronze_trans')}}
),

unpacked as (
    select * from temp
)

select * from unpacked