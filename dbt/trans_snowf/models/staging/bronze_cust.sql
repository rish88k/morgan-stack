{{ config(
    materialized='incremental', 
    unique_key='customer_id'
) }}

with raw_source as (
    select * from {{ source('raw_data', 'RAW_CUSTOMERS') }}
),

unpacked as (
    select
        -- Metadata for CDC
        raw_json:"payload":"op"::STRING as operation_type,
        
        -- Pulling the timestamp from the 'source' block for consistency
        raw_json:"payload":"source":"ts_ms"::BIGINT as event_timestamp_ms,

        -- Customer Data (Case-sensitive JSON keys)
        raw_json:"payload":"after":"customer_id"::UUID as customer_id,
        raw_json:"payload":"after":"first_name"::STRING as first_name,
        raw_json:"payload":"after":"last_name"::STRING as last_name,
        raw_json:"payload":"after":"email"::STRING as email,
        raw_json:"payload":"after":"phone_number"::STRING as phone_number,
        raw_json:"payload":"after":"address"::STRING as address,
        raw_json:"payload":"after":"city"::STRING as city,
        raw_json:"payload":"after":"country"::STRING as country,
        
        -- Source Timestamps
        raw_json:"payload":"after":"created_at"::TIMESTAMP_TZ as created_at,
        raw_json:"payload":"after":"updated_at"::TIMESTAMP_TZ as updated_at,

        -- Audit Info
        raw_json:"payload":"source":"lsn"::INT as postgres_lsn
    from raw_source
)

select * from unpacked

{% if is_incremental() %}
  where event_timestamp_ms > (select max(event_timestamp_ms) from {{ this }})
{% endif %}