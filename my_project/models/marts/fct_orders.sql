{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

with orders as (
    select
        json_data:id::integer as order_id,
        json_data:quantity::integer as quantity,
        json_data:customer_id::integer as customer_id,
        json_data:product_id::integer as product_id,
        inserted_at as order_date
    from {{ source('raw_data', 'RAW_TABLE_ORDERS') }}

    {% if is_incremental() %}
        where inserted_at >= (select max(order_date) from {{ this }})
    {% endif %}
),

customers as (
    select * from {{ ref('customers_snapshot') }}
),

products as (
    select *from {{ ref('products_snapshot') }}
)

select
    o.order_id,
    o.order_date,
    o.quantity,
    c.dbt_scd_id as customer_version_key,
    p.dbt_scd_id as product_version_key,
    o.customer_id,
    o.product_id
from orders o
left join customers c
    on o.customer_id = c.customer_id
    and o.order_date >= c.dbt_valid_from
    and o.order_date < coalesce(c.dbt_valid_to, '9999-12-31'::timestamp)
left join products p
    on p.product_id = p.product_id
    and o.order_date >= p.dbt_valid_from
    and o.order_date < coalesce(p.dbt_valid_to, '9999-12-31'::timestamp)