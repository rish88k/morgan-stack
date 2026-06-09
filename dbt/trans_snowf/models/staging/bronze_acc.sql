{{ config(
    materialized='incremental', 
    unique_key='account_id'
) }}

with raw_source as (
    select * from {{ source('raw_data', 'RAW_ACCOUNTS') }}
),

unpacked as (
    select
        -- Metadata for CDC
        raw_json:"payload":"op"::STRING as operation_type,
        raw_json:"payload":"source":"ts_ms"::BIGINT as event_timestamp_ms,

        -- Account Data
        raw_json:"payload":"after":"account_id"::UUID as account_id,
        raw_json:"payload":"after":"customer_id"::UUID as customer_id,
        raw_json:"payload":"after":"account_number"::STRING as account_number,
        raw_json:"payload":"after":"account_type"::STRING as account_type,
        
        -- Note: 'balance' might be Base64 if it's a Decimal in Debezium. 
        -- If so, use ::STRING and decode later, otherwise use ::DECIMAL
        -- raw_json:"payload":"after":"balance"::DECIMAL(15,2) as balance,
        {{ decode_base64_safe('raw_json:"payload":"after":"balance"::STRING', 2) }} AS balance,
        -- TRY_TO_DECIMAL(
        --        BASE64_DECODE_BINARY(raw_json:"payload":"after":"balance"::STRING),
        --       0
           -- )::STRING, 
           -- 15, 2
           --) as balance,

        raw_json:"payload":"after":"currency"::STRING as currency,
        raw_json:"payload":"after":"status"::STRING as status,

        -- Source Timestamp
        raw_json:"payload":"after":"created_at"::TIMESTAMP_TZ as created_at,

        -- Audit Info
        raw_json:"payload":"source":"lsn"::INT as postgres_lsn
    from raw_source
)

select * from unpacked

{% if is_incremental() %}
  where event_timestamp_ms > (select max(event_timestamp_ms) from {{ this }})
{% endif %}