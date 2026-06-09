

{{ config(
    materialized='incremental', 
    unique_key='transaction_id'
) }}

with raw_source as (
    select * from {{ source('raw_data', 'RAW_TRANSACTIONS') }}
),

unpacked as (
    select
        -- Accessing keys inside the 'raw_json' variant column
        raw_json:"payload":"op"::STRING as operation_type,
        
        -- Primary & Foreign Keys
        raw_json:"payload":"after":"transaction_id"::UUID as transaction_id,
        raw_json:"payload":"after":"account_id"::UUID as account_id,

        -- Transaction Details
        raw_json:"payload":"after":"transaction_type"::STRING as transaction_type,
        
        -- Debezium sends Decimals as Base64 bytes (e.g., "TK8="). 
        -- We cast to string first; you'll need a UDF or logic later to get the numeric value.
        raw_json:"payload":"after":"amount"::STRING as amount_raw,
        {{ decode_base64_safe('raw_json:"payload":"after":"amount"::STRING', 2) }} as amount_converted,
        
        raw_json:"payload":"after":"description"::STRING as description,
        raw_json:"payload":"after":"merchant_name"::STRING as merchant_name,
        
        -- Timestamps
        raw_json:"payload":"after":"transaction_date"::TIMESTAMP_TZ as transaction_date,

        -- Required for your INCREMENTAL logic below
        raw_json:"payload":"source":"ts_ms"::BIGINT as event_timestamp_ms,

        -- Audit Info
        raw_json:"payload":"source":"lsn"::INT as postgres_lsn
    from raw_source
)

select * from unpacked

{% if is_incremental() %}
  -- Now 'event_timestamp_ms' exists in the 'unpacked' CTE above
  where event_timestamp_ms > (select max(event_timestamp_ms) from {{ this }})
{% endif %}