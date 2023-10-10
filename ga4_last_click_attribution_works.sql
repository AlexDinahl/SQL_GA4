--https://tanelytics.com/ga4-bigquery-session-traffic_source/

with date_range as (
  select
    '20231005' as start_date,
    '20231005' as end_date),
lookup_range as (select
    format_date('%Y%m%d',date_sub(parse_date('%Y%m%d',start_date), interval 30 day)) as lookup_start_date,
    end_date as lookup_end_date
    from date_range),
events as (
  select
    cast(event_date as date format 'YYYYMMDD') as date,
    -- unique session id
    concat(user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
    user_pseudo_id,
    (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_start,
    -- wrap all traffic source dimensions into a struct for the next step
    (
      select
        as struct collected_traffic_source.manual_source as source,
        collected_traffic_source.manual_medium as medium,
        collected_traffic_source.manual_campaign_name as campaign,
        collected_traffic_source.gclid as gclid

    ) as traffic_source,
  --We collect first traffic source to apply it for empty ones, when the lookup window is not covering it
        (
      select
        as struct traffic_source.source as first_source,
                  traffic_source.medium as first_medium,
                  traffic_source.name as first_campaign
    ) as first_traffic_source,
    event_timestamp
  from
    `isg-dwh-bigquery.analytics_292798251.events_*`,date_range,lookup_range
  where
    (_table_suffix >= lookup_start_date and _table_suffix <= lookup_end_date)
    and event_name not in ('session_start', 'first_visit')
),
sessions as (
select
  min(date) as date,
  session_id,
  user_pseudo_id,
  session_start,
  -- the traffic source of the first event in the session with session_start and first_visit excluded
  array_agg(
    if(
      coalesce(traffic_source.source, traffic_source.medium, traffic_source.campaign, traffic_source.gclid) is not null,
      (
        select
          as struct if(traffic_source.gclid is not null, 'google', traffic_source.source) as source,
            if(traffic_source.gclid is not null, 'cpc', traffic_source.medium) as medium,
            traffic_source.campaign,
            traffic_source.gclid
      ),
      null
    )
    order by
      event_timestamp asc
    limit
      1
  ) [safe_offset(0)] as session_first_traffic_source,
  -- the last not null traffic source of the session
  array_agg(
    if(
      coalesce(traffic_source.source,traffic_source.medium,traffic_source.campaign,traffic_source.gclid) is not null,
      (
        select
          as struct if(traffic_source.gclid is not null, 'google', traffic_source.source) as source,
            if(traffic_source.gclid is not null, 'cpc', traffic_source.medium) as medium,
            traffic_source.campaign,
            traffic_source.gclid
      ),
      null
    ) ignore nulls
    order by
      event_timestamp desc
    limit
      1
  ) [safe_offset(0)] as session_last_traffic_source,
 array_agg(
    if(
      coalesce(first_traffic_source.first_source,first_traffic_source.first_medium,first_traffic_source.first_campaign) is not null,
      traffic_source,
      null
    ) ignore nulls
    order by
      event_timestamp desc
    limit
      1
  ) [safe_offset(0)] as first_visit_traffic_source, 
from
  events
where
  session_id is not null
group by
  session_id,
  user_pseudo_id,
  session_start
  ),
attribution as (
  select date
  --Here if session_first_traffic_source is null take session_last_traffic_source within lookup window which is not direct, and finally if this one is null take the very first one
  ,ifnull(ifnull(
    session_first_traffic_source,
    last_value(session_last_traffic_source ignore nulls) over(
      partition by user_pseudo_id
      order by
        session_start range between 2592000 preceding
        and 1 preceding -- 30 day lookback
    )
  ),first_visit_traffic_source) as last_non_direct,session_id
  from sessions
  )
  select concat(ifnull(last_non_direct.source,'(direct)'),' / ',ifnull(last_non_direct.medium,'(none)')) as sm,count(distinct session_id) as sessions,hll_count.extract(hll_count.init(session_id,12)) as approx_sessions
  from attribution,date_range
  where date between parse_date('%Y%m%d',start_date) and parse_date('%Y%m%d',end_date)
  group by 1
  order by 2 desc
