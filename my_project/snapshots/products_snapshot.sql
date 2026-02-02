{% snapshot products_snapshot %}

{{
    config(
        target_database='POC2_DB_FINAL',
        target_schema='SNAPSHOTS',
        unique_key='product_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=False
    )
}}

with source_data as (
    select
        json_data:id::integer as product_id,
        json_data:name::string as name,
        json_data:price::number(38, 2) as price,
        inserted_at as updated_at
    from {{ source('raw_data', 'RAW_TABLE_PRODUCTS') }}
)

select * from source_data

{% if is_incremental() %}
    where updated_at >= (
        select DATEADD(minute, -30, max(dbt_valid_from))
        from {{ this }}
    )
{% endif %}

QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY updated_at DESC) = 1

{% endsnapshot %}