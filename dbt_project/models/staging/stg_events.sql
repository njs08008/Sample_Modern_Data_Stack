select
    event_id,
    event_type,
    event_ts,
    ingested_at,
    user_id,
    country,
    page,
    query,
    amount,
    date(event_ts) as event_date
from read_parquet('../data/silver/events/**/*.parquet')