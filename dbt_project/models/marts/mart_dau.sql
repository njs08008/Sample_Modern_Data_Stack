select
    event_date,
    count(distinct user_id) as daily_active_users
from {{ ref('stg_events') }}
group by event_date