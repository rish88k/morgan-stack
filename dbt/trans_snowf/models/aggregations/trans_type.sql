{{ config(materialized='table') }}

with temp as (
    select * from {{ ref('bronze_trans') }}
),

unpacked as (
    select
        count(transaction_id) as no_of_transactions,
        transaction_type
    from temp
    group by transaction_type
)

select * from unpacked
