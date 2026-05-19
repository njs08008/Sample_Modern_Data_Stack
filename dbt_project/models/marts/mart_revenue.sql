select
    event_date,
    sum(amount) as daily_revenue
from {{ ref('stg_events') }}
where event_type = 'purchase'
group by event_date