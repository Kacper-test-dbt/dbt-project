select *
from {{ ref('fct_orders') }}
where quantity <= 0