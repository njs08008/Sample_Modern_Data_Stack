select
    event_date,
    event_type,
    count(*) as event_count
from {{ ref('stg_events') }}
group by event_date, event_type