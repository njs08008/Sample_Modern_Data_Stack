with events as (

    select *
    from {{ ref('stg_events') }}

),

searches as (
    select
        user_id,
        event_date,
        count(*) as search_events
    from events
    where event_type = 'search'
    group by user_id, event_date
),

page_views as (
    select
        user_id,
        event_date,
        count(*) as page_view_events
    from events
    where event_type = 'page_view'
    group by user_id, event_date
),

purchases as (
    select
        user_id,
        event_date,
        count(*) as purchase_events
    from events
    where event_type = 'purchase'
    group by user_id, event_date
),

joined as (
    select
        coalesce(s.user_id, p.user_id, v.user_id) as user_id,
        coalesce(s.event_date, p.event_date, v.event_date) as event_date,
        coalesce(search_events, 0) as searches,
        coalesce(page_view_events, 0) as page_views,
        coalesce(purchase_events, 0) as purchases
    from searches s
    full outer join page_views v
        on s.user_id = v.user_id and s.event_date = v.event_date
    full outer join purchases p
        on coalesce(s.user_id, v.user_id) = p.user_id
        and coalesce(s.event_date, v.event_date) = p.event_date
)

select * from joined