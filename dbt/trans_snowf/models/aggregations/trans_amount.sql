{{ config(materialized='table') }}

with temp as (
    select * from {{ ref('bronze_trans') }}
),

unpacked as (
    select
        transaction_id,
        amount_raw as amount,
        case
            when amount > 0    and amount < 200  then 'rainbet'
            when amount >= 200 and amount < 1000 then 'grocers'
            when amount >= 1000                  then 'poker'
            else null
        end as category
    from temp
),

final as (
    select
        count(transaction_id) as no_of_transactions,
        category
    from unpacked           -- fixed: was referencing 'temp' instead of 'unpacked'
    group by category
    order by no_of_transactions
)

select * from final
