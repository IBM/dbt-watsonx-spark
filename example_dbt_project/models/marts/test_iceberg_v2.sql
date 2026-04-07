{{
    config(
        materialized='table',
        file_format='iceberg',
        tags=['test', 'iceberg-v2']
    )
}}

-- Test model for Iceberg v2 table support
-- This verifies the fix for "SHOW TABLE EXTENDED is not supported for v2 tables"

with source_data as (
    select
        1 as order_id,
        101 as customer_id,
        100.50 as amount,
        'completed' as status
    union all
    select
        2 as order_id,
        102 as customer_id,
        250.75 as amount,
        'pending' as status
    union all
    select
        3 as order_id,
        101 as customer_id,
        75.25 as amount,
        'completed' as status
)

select
    order_id,
    customer_id,
    amount,
    status,
    current_timestamp() as processed_at
from source_data

-- 
