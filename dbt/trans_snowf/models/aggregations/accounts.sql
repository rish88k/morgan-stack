{{ config(materialized='table') }}

with temp as (
    select * from {{ ref('bronze_acc') }}
),

unpacked as (
    select
        account_id,
        balance,
        case
            when balance between 0   and 200  then 'broke'
            when balance between 200 and 1000 then 'BrokeByMonday'
            when balance > 1000               then 'homeless'
            else null
        end as category
    from temp
),

temp2 as (
    select
        count(account_id) as no_of_accounts,
        category          as poorness
    from unpacked
    group by category
)

select * from temp2
