with date_range as (
  select
    '20230326' as start_date,
    '20230326' as end_date),
ua as (    
          select distinct parse_date('%Y%m%d',date) as visit_date
                    ,fullvisitorId
                    ,concat(fullvisitorId,'.',visitId) as sid
                    ,(select cd.value from unnest(h.customDimensions) cd where cd.index = 12) as client_id
                    ,visitStartTime
                  from `isg-dwh-bigquery.206405060.ga_sessions_*`, unnest(hits) as h, date_range
                  
                  where _table_suffix between start_date and end_date),
ga as (
            select
    cast(event_date as date format 'YYYYMMDD') as visit_date,
    -- unique session id
    concat(user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
    user_pseudo_id,
    -- ga_session_id is the unix timestamp in seconds when the session started
    (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_start
  from
    `isg-dwh-bigquery.analytics_292798251.events_*`,date_range
  where _table_suffix between start_date and end_date

)
select count(distinct fullvisitorId) as ua_users,count(distinct user_pseudo_id) as ga4_users
from ua
left join ga
on client_id = user_pseudo_id
