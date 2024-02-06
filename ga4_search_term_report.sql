declare reporting_timezone string default 'Europe/Berlin';

create temp function GetParamValue(params any type, target_key string)
as (
  (select `value`from unnest(params) where key = target_key limit 1)
);


/*

with date_range as (
  select
    '20231212' as start_date,
    '20231212' as end_date),
purchases as (
             select
              date(datetime(timestamp_micros(event_timestamp), reporting_timezone)) as visit_date,
              event_timestamp,
              concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as sid,
              ecommerce.transaction_id as order_number,
              sum(if(ecommerce.transaction_id<>'(not set)',1,0)) as purchases,
              round(sum(ifnull(item_revenue,0)),2) as item_revenue,
              --round(sum(ifnull(GetParamValue(item_params, 'item_discount').int_value,0)),2) as item_discount
                
 from `isg-dwh-bigquery.analytics_292798251.events_*` ,date_range,unnest(items) as items
 where  _table_suffix between start_date and end_date
       and event_name='purchase'
group by 1,2,3,4
),
searches as (
select  distinct
        date(datetime(timestamp_micros(event_timestamp), reporting_timezone)) as visit_date,
        user_pseudo_id,
        collected_traffic_source.manual_source as traffic_source,
        event_timestamp,
        device.category, 
        concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as sid,
        rtrim(ltrim(GetParamValue(event_params, 'search_term').string_value,' '),' ') as keyword,
        GetParamValue(event_params, 'unique_search_term').int_value as unique_keyword,
        GetParamValue(event_params, 'query_parameter').string_value as query_parameter,
        --GetParamValue(event_params, 'page_location').string_value as page,
        if( instr(GetParamValue(event_params, 'page_location').string_value,'?',1)>0,
              regexp_extract(GetParamValue(event_params, 'page_location').string_value,r'^https?:\/\/(.*)?\?'),
              regexp_extract(GetParamValue(event_params, 'page_location').string_value,r'^https?:\/\/(.*)?')
              ) as url,
        row_number() over (partition by concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) order by event_timestamp desc) as search_step_sess,
        row_number() over (partition by concat(event_date,user_pseudo_id) order by event_timestamp desc) as search_step_user,
from `isg-dwh-bigquery.analytics_292798251.events_*`,date_range
where _table_suffix between start_date and end_date
      and event_name='view_search_results' and GetParamValue(event_params, 'page_location').string_value like ('%/suche/%')
),
searches_agg as (
select s.visit_date,
       user_pseudo_id,
       traffic_source,
       keyword,
       s.sid,
       s.event_timestamp,
       --s.search_step,
       sum(unique_keyword) as unique_keyword,
       item_revenue,
       order_number
from searches as s
left join purchases as p on s.sid=p.sid and s.event_timestamp<p.event_timestamp and search_step_sess=1 and search_step_user=1
group by 1,2,3,4,5,6,8,9
order by user_pseudo_id,event_timestamp
)
select visit_date,
       keyword,
       sum(unique_keyword) as total_unique_searches,
       count(distinct order_number) as transactions, 
      ifnull(sum(item_revenue),0) as revenue
from searches_agg
group by 1,2
order by  3 desc
--user_pseudo_id,sid,event_timestamp
;
*/



--if(instr(device.web_info.hostname,'www.',1)>0,device.web_info.hostname,regexp_extract(device.web_info.hostname,r'\.([a-z]+\.[a-z]{2})$')) as hostname,






---Last Click as in UA

--/*

with date_range as (
  select
    '20231212' as start_date,
    '20231212' as end_date),
usrs as (
select  date(datetime(timestamp_micros(event_timestamp), reporting_timezone)) as visit_date,
        user_pseudo_id,
        event_timestamp,
        collected_traffic_source.manual_source as traffic_source, 
        concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as sid,
        GetParamValue(event_params, 'search_term').string_value as keyword,
        GetParamValue(event_params, 'unique_search_term').int_value as unique_keyword,
        if(ecommerce.transaction_id is not null, 
                  last_value(GetParamValue(event_params, 'search_term').string_value ignore nulls) over(partition by concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) order by event_timestamp),GetParamValue(event_params, 'search_term').string_value) as kw,
        if(ecommerce.transaction_id is not null, 
                  last_value(
                     if(GetParamValue(event_params, 'search_term').string_value is not null
                     ,regexp_replace(replace(GetParamValue(event_params, 'page_location').string_value,concat('https://',device.web_info.hostname),''),r'[\?].*','')
                        ,null)
                         ignore nulls) 
                         over(partition by concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) order by event_timestamp)
                         ,regexp_replace(replace(GetParamValue(event_params, 'page_location').string_value,concat('https://',device.web_info.hostname),''),r'[\?].*','')) as kw_page,
        ecommerce.transaction_id as order_number,
        ecommerce.purchase_revenue as purchase_revenue,
        --ifnull(ecommerce.purchase_revenue,0) as purchase_revenue
  from `isg-dwh-bigquery.analytics_292798251.events_*`,date_range
  where _table_suffix between start_date and end_date
        and event_name in ('view_search_results','purchase','page_view')
),
attr as (
select usrs.*
from usrs
where 
--user_pseudo_id in (select distinct user_pseudo_id from usrs where kw='Shimano Nexus Revoshift SL-C3000 7-fach') 
sid in (select distinct sid from usrs where order_number is not null)
group by 1,2,3,4,5,6,7,8,9,10,11
order by user_pseudo_id,sid,event_timestamp
)
select rtrim(ltrim(kw,''),'') as kw
,kw_page as page_path
,sum(unique_keyword) as total_unique_searches,count(distinct order_number) as orders,round(sum(purchase_revenue),2) as revenue
from attr
--where page_type='search_page' and order_number is not null
group by 1,2 
order by 3 desc



--*/
