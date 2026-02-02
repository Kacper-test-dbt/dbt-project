{% snapshot customers_snapshot %}

{{
    config(
        target_database='POC2_DB_FINAL',
        target_schema='SNAPSHOTS',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=False
    )
}}

with source_data as (
    select
        json_data:id::integer as customer_id,
        json_data:first_name::string as first_name,
        json_data:last_name::string as last_name,
        json_data:city::string as city,
        inserted_at as updated_at
    from {{ source('raw_data', 'RAW_TABLE_CUSTOMERS') }}
)

select * from source_data

{% if is_incremental() %}
    where updated_at >= (
        select DATEADD(minute, -30, max(dbt_valid_from))
        from {{ this }}
    )
{% endif %}

QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1

{% endsnapshot %}