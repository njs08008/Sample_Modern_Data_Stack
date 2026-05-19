select *
from {{ source('warehouse', 'dim_user_scd') }}
where is_current = true