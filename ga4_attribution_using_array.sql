--https://support.google.com/analytics/answer/13504892?hl=en#zippy=%2Cin-this-article
--https://stackoverflow.com/questions/73835404/bigquery-check-if-string-values-contain-elements-of-an-array-or-subquery
--https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#string_agg
--https://support.google.com/analytics/answer/13504892?hl=en#zippy=%2Cin-this-article

create temp function GetParamValue(params any type, target_key string)
as (
  (select `value`from unnest(params) where key = target_key limit 1)
);

/*

with date_range as (
  select
    '20231101' as start_date,
    '20231119' as end_date),
chains as (
  select
    cast(event_date as date format 'YYYYMMDD') as visit_date,
    user_pseudo_id,
    GetParamValue(event_params, 'ga_session_id').int_value as session_id,
    string_agg(distinct ifnull(collected_traffic_source.manual_source,'(direct)'),',' ) as utm_source,
    array_length(regexp_extract_all(string_agg(distinct ifnull(collected_traffic_source.manual_source,'(none)'),','), ",")) AS size,
  from `isg-dwh-bigquery.analytics_292798251.events_*`,date_range
  where _table_suffix between start_date and end_date
  --and user_pseudo_id='1243638799.1699126965'
  group by 1,2,3
  order by visit_date)
  select visit_date--,user_pseudo_id,utm_source,size
  ,if(split(utm_source,',')[safe_offset(size)]='(direct)' and size>0,split(utm_source,',')[safe_offset(size-1)],split(utm_source,',')[safe_offset(size)]) as last_click
  ,count(distinct concat(user_pseudo_id,session_id)) as sessions
  from chains
  group by 1,2;
*/


with date_range as (
  select
    '20240107' as start_date,
    '20240107' as end_date),
channels as (
select distinct
        cast(event_date as date format 'YYYYMMDD') as visit_date,
        event_timestamp,
        user_pseudo_id,
        event_name,
        GetParamValue(event_params, 'ga_session_id').int_value as session_id,
        if(collected_traffic_source.manual_source is null and collected_traffic_source.gclid is not null,
                                                  'google',collected_traffic_source.manual_source) as utm_source
  from `isg-dwh-bigquery.analytics_292798251.events_*`,date_range
  where _table_suffix between start_date and end_date),
chains as (
select visit_date
      ,user_pseudo_id
      ,session_id
      ,utm_source
      ,string_agg(utm_source,',') over (partition by user_pseudo_id order by session_id,event_timestamp) as usource
      ,instr(string_agg(event_name,',') over (partition by concat(user_pseudo_id,session_id) order by session_id,event_timestamp),'session_start') as event
      ,array_length(regexp_extract_all(string_agg(utm_source,',') over (partition by user_pseudo_id order by session_id,event_timestamp),',')) as size
from channels),
attribution as (
select visit_date
      ,user_pseudo_id
      ,session_id
      --,utm_source
      ,usource
      ,case when split(usource,',')[safe_offset(size)] is null and event=0 then '(not set)'
            when split(usource,',')[safe_offset(size)] is null and event>0 then '(direct)'
            else  split(usource,',')[safe_offset(size)] end as last_click_source
      ,event
      --,size
      --,count(distinct concat(user_pseudo_id, session_id)) as sessions
      --,hll_count.extract(hll_count.init(concat(user_pseudo_id, session_id),12))
from chains
--group by ,2
order by user_pseudo_id,session_id)
select last_click_source,count(distinct concat(user_pseudo_id, session_id)) as sessions
from attribution
where visit_date='2024-01-07' --and instr(usource,'connexity')=1
group by 1
order by sessions desc;









--source_medium checks
/*
with date_range as (
  select
    '20231119' as start_date,
    '20231119' as end_date),
page as (
select distinct
if(GetParamValue(event_params, 'page_location').string_value='(not set)',null,GetParamValue(event_params, 'page_location').string_value) as page,
concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as session_id,
collected_traffic_source.manual_source  as utm_source,
collected_traffic_source.manual_medium  as utm_medium,
collected_traffic_source.manual_campaign_name  as utm_campaign,
collected_traffic_source.manual_term as term,
collected_traffic_source.manual_content as content,
collected_traffic_source.gclid as gclid,
collected_traffic_source.dclid as dclid,
collected_traffic_source.srsltid  as srsltid
  from `isg-dwh-bigquery.analytics_292798251.events_*`,date_range
  where _table_suffix between start_date and end_date)
select if(page is null,0,1) as page,count(distinct session_id)
from page
group by 1
*/
