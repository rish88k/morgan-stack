{{ config(materialized='table') }}

with temp as (
    select * from {{ ref('bronze_trans') }}
),

transformed as (
    select
        count(transaction_id) as no_of_transactions,
        transaction_date
    from temp
    group by transaction_date
    order by transaction_date
)

select * from transformed
