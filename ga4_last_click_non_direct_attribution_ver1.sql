-- select the first event that is not a session_start 
-- or first_visit event from each session
with fn as (
with tot as (
with 
--This date range defines the amount of data to take for attribution. 
--As a lookup window 30 days for attribution. Minimum 30 days must be taken.
--Ideally there must be no date range limit, but this range is set to artificially limit the data consumption from GA
--Otherwise it will be terabytes of data
date_range as (
  select
    '20230101' as start_date,
    '20230131' as end_date),
events as (
  select
    cast(event_date as date format 'YYYYMMDD') as date,
    -- unique session id
    concat(user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
    user_pseudo_id,
    -- ga_session_id is the unix timestamp in seconds when the session started
    (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_start,
    (select value.string_value from unnest(event_params) where key = 'source') as source,
    (select value.string_value from unnest(event_params) where key = 'medium') as medium,
    (select value.string_value from unnest(event_params) where key = 'campaign')  as campaign,
    (select value.string_value from unnest(event_params) where key = 'gclid') as gclid,
    -- flag the session's first event
    if(
      row_number() over(
        partition by concat(user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id'))
        order by
          event_timestamp asc
      ) = 1,
      true,
      false
    ) as session_first_event
  from
    `bigquery.analytics_123456789.events_*`,date_range
  where _table_suffix between start_date and end_date and
    event_name not in ('session_start', 'first_visit') qualify session_first_event = true
    
)
select
  date,
  session_id,
  user_pseudo_id,
  session_start,
  -- wrap the session details in a struct to make it easier 
  -- to identify sessions with traffic source data
  if(
    source is not null
    or medium is not null
    or campaign is not null
    or gclid is not null,
    (
      select
        as struct if(
          gclid is not null,
          'google',
          source
        ) as source,
        if(
          gclid is not null,
          'cpc',
          medium
        ) as medium,
        campaign,
        gclid
    ),
    null
  ) as session_traffic_source
from
  events)
select
  *,
  --This is where attribution is calculated
  ifnull(
    session_traffic_source,
    last_value(session_traffic_source ignore nulls) over(
      partition by user_pseudo_id
      order by
      --This is a lookup window of 30 days given in seconds
        session_start range between 2592000 preceding
        and current row
    )
  ) as session_traffic_source_last_non_direct,
from tot
order by tot.user_pseudo_id,session_id
)
select
  session_traffic_source_last_non_direct.source as source,
  session_traffic_source_last_non_direct.medium as medium,
  count(distinct session_id) as sessions
from
  fn
where
  date between '2023-01-31' and '2023-01-31'
group by
  1,
  2
order by
  sessions desc, source;













#######################################SAME BUT WITH CHANNEL_DETAIL#####################################

#https://tanelytics.com/ga4-bigquery-session-traffic_source/ 

/*
-- select the first event that is not a session_start 
-- or first_visit event from each session
with fn as (
with tot as (
with 
--This date range defines the amount of data to take for attribution. 
--As a lookup window 30 days for attribution. Minimum 30 days must be taken.
--Ideally there must be no date range limit, but this range is set to artificially limit the data consumption from GA
--Otherwise it will be terabytes of data
date_range as (
  select
    '20230101' as start_date,
    '20230131' as end_date),
events as (
  select
    cast(event_date as date format 'YYYYMMDD') as date,
    -- unique session id
    concat(user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
    user_pseudo_id,
    -- ga_session_id is the unix timestamp in seconds when the session started
    (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_start,
    (select value.string_value from unnest(event_params) where key = 'source') as source,
    (select value.string_value from unnest(event_params) where key = 'medium') as medium,
    (select value.string_value from unnest(event_params) where key = 'campaign')  as campaign,
    (select value.string_value from unnest(event_params) where key = 'gclid') as gclid,
    (select value.string_value from unnest(event_params) where key = 'channel_detail') as channel_detail,
    -- flag the session's first event
    if(
      row_number() over(
        partition by concat(user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id'))
        order by
          event_timestamp asc
      ) = 1,
      true,
      false
    ) as session_first_event
  from
    `bigquery.analytics_123456789.events_*`,date_range
  where _table_suffix between start_date and end_date and
    event_name not in ('session_start', 'first_visit') qualify session_first_event = true
    
)
select
  date,
  session_id,
  user_pseudo_id,
  session_start,
  -- wrap the session details in a struct to make it easier 
  -- to identify sessions with traffic source data
  if(
    source is not null
    or medium is not null
    or campaign is not null
    or gclid is not null
    or channel_detail is not null,
    (
      select
        as struct if(
          gclid is not null,
          'google',
          source
        ) as source,
        if(
          gclid is not null,
          'cpc',
          medium
        ) as medium,
        campaign,
        gclid,
        channel_detail,
        
    ),
    null
  ) as session_traffic_source
from
  events)
select
  *,
  --This is where attribution is calculated
  ifnull(
    session_traffic_source,
    last_value(session_traffic_source ignore nulls) over(
      partition by user_pseudo_id
      order by
      --This is a lookup window of 30 days given in seconds
        session_start range between 2592000 preceding
        and current row
    )
  ) as session_traffic_source_last_non_direct,
from tot
order by tot.user_pseudo_id,session_id
)
select
  session_traffic_source_last_non_direct.source as source,
  session_traffic_source_last_non_direct.medium as medium,
  count(distinct session_id) as sessions
from
  fn
where
  date between '2023-01-31' and '2023-01-31'
group by
  1,
  2
order by
  sessions desc, source
*/
