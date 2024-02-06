declare reporting_timezone string default 'Europe/Berlin';

create temp function GetParamValue(params any type, target_key string)
as (
  (select `value`from unnest(params) where key = target_key limit 1)
);

create temp function SearchEngineAndSocial(target_key string)
as (
  case when regexp_contains(target_key,
       r'www\.google|yandex|search|startpage|duckduckgo|ecosia|bing|go\.mail\.ru|\.aol\.|baidu|suche\.|eniro|rambler|altavista|360\.cn|alice\.com|alltheweb|daum') then 1 
       when regexp_contains(target_key,r'instagram|facebook|twitter|t\.co|reddit|youtube|linkedin|pinterest|gutefrage|snapchat|blog\.naver\.com|vk\.com|xing|blogger|instapaper|wer\-weiss\-was|yelp|wordpress|researchgate|disqus') then 2
       else 0 end
);
--GETFULLVISITORS

/*

with tot as (
with date_range as (
  select
    '20231212' as start_date,
    '20231212' as end_date)
select
    concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as sid,
    --if(regexp_contains(user_pseudo_id,r'[aA-zZ]'),concat(cast(floor(pow(10,10)*rand()) as int64),'.',cast(floor(pow(10,10)*rand()) as int64)),user_pseudo_id) as eid,
    user_pseudo_id,
    datetime(timestamp_micros(min(event_timestamp)), reporting_timezone) as visit_time,
    if(GetParamValue(event_params, 'ga_session_number').int_value=1 and event_name = 'session_start',1,0)  as new_visitor,
    device.operating_system_version as os,
    concat(device.web_info.browser,' ',device.web_info.browser_version) as browser,
    device.language as browser_lang,
    geo.city,
    geo.region,
    geo.country,
    if(device.category='desktop','any Desktop & Laptop'
            ,ifnull(concat(device.mobile_brand_name,' ',device.mobile_marketing_name)
                          ,ifnull(concat(device.mobile_brand_name,' ',device.mobile_model_name),'Unknown'))) as mobile_client,
    datetime(timestamp_micros(max(event_timestamp)), reporting_timezone) as visit_time_end,
    if(instr(device.web_info.hostname,'www.',1)=0,device.web_info.hostname,regexp_extract(device.web_info.hostname,r'www.([a-z]+\.[a-z]{2,}|[a-z]{2,}\.[a-z]+\.[a-z]{2,})$')) as source_name,
    device.web_info.hostname
  from `isg-dwh-bigquery.analytics_263765711.events_*`,date_range
  where _table_suffix between start_date and end_date
  and not regexp_contains(device.web_info.hostname,r'staging|development|appspot')
group by 1,2,4,5,6,7,8,9,10,11,13,14
)
select source_name,hostname,count(distinct sid)
from tot 
group by 1,2
order by 3 desc
*/

--GETCONTENTS

/*
with tot  as(
with date_range as (
  select
    '20231119' as start_date,
    '20231119' as end_date)
select
    concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as sid,
    format_timestamp('%Y-%m-%d %H:%M:%S',datetime(timestamp_micros(event_timestamp), reporting_timezone)) as times,
    --datetime(timestamp_micros(lead(event_timestamp, 1) over (partition by concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) order by event_timestamp asc)), reporting_timezone) as n,
    case 
    when GetParamValue(event_params, 'page_location').string_value like ('%/404/%') then 'Error Page 404'
    when GetParamValue(event_params, 'page_location').string_value like ('%/warenkorb/%') or GetParamValue(event_params, 'page_location').string_value like ('%/cart/%')
    then 'Cart Page'
    when GetParamValue(event_params, 'page_location').string_value like ('%/suche/%') or GetParamValue(event_params, 'page_location').string_value like ('%/search/%')
    then 'Search Page'
    when event_name='purchase' then 'Thank You Page'
    else ifnull(GetParamValue(event_params, 'page_type').string_value,'Unknown') end
    as content,
   cast(round( ifnull((lead(event_timestamp, 1) over 
                      (partition by concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) order by event_timestamp asc) 
                                      - event_timestamp)/pow(10,6),0),0) as numeric) AS page_duration,
      row_number() over (partition by concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) order by event_timestamp asc) AS page_order,  
    --GetParamValue(event_params, 'page_location').string_value
    if(instr(device.web_info.hostname,'www.',1)=0,device.web_info.hostname,regexp_extract(device.web_info.hostname,r'www.([a-z]+\.[a-z]{2,}|[a-z]{2,}\.[a-z]+\.[a-z]{2,})$')) as source_name
    from `isg-dwh-bigquery.analytics_292798251.events_*`,date_range
  where _table_suffix between start_date and end_date
  and GetParamValue(event_params, 'environment').string_value='Production' --not regexp_contains(device.web_info.hostname,r'staging|development')
  and event_name in ('page_view','purchase') --and coalesce(GetParamValue(event_params, 'page_type').string_value) is null
--group by 1,2,3,5,6
order by sid,times
)

select content,count(distinct sid) from tot
group by 1
order by 2
*/



--GETFULLBASKET
--/*
with date_range as (
  select
    '20231127' as start_date,
    '20231127' as end_date)
select
    concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as sid,
    concat(items.item_name,'_',items.item_id,'_',GetParamValue(item_params, 'item_variation').int_value) as product,
    items.quantity as quantity,
    items.price as price,
    if(event_name='add_to_cart',1,0) as stat,
    format_timestamp('%Y-%m-%d %H:%M:%S',datetime(timestamp_micros(event_timestamp), reporting_timezone)) as times,
    row_number() over (partition by user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value order by event_timestamp) as bas_pos
from `isg-dwh-bigquery.analytics_292798251.events_*`,unnest(items) as items,date_range
where _table_suffix between start_date and end_date
  and event_name in('items_in_cart') --'add_to_cart',
  --and items.item_name like '%Sommet 297%'
;
--*/

--GETFULLCAMPAIGNS

/*
with date_range as (
  select
    '20231119' as start_date,
    '20231119' as end_date)
select distinct
    concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as sid,
    min(format_timestamp('%Y-%m-%d %H:%M:%S',datetime(timestamp_micros(event_timestamp), reporting_timezone))) as times,
    string_agg( distinct concat(
          'utm_source=',
    if(collected_traffic_source.manual_source is null and collected_traffic_source.gclid is not null,
                'google',collected_traffic_source.manual_source),
          '&utm_medium=',
    if(collected_traffic_source.manual_medium is null and collected_traffic_source.gclid is not null,
                'cpc',collected_traffic_source.manual_medium),
          
    ifnull(concat('&utm_campaign=',if(instr(collected_traffic_source.manual_campaign_name,collected_traffic_source.manual_medium,1)>0,null,collected_traffic_source.manual_campaign_name)),''),          
    ifnull(concat('&utm_content=',collected_traffic_source.manual_content),'')
    ) limit 1) as campaign,
    string_agg( distinct     ifnull(if(collected_traffic_source.manual_term='(not provided)',null,collected_traffic_source.manual_term),'') limit 1) as keyword,
    regexp_extract(device.web_info.hostname,r'\.([a-z]+\.[a-z]{2,})$') as source_name

from `isg-dwh-bigquery.analytics_292798251.events_*`,date_range
where _table_suffix between start_date and end_date
and not regexp_contains(device.web_info.hostname,r'staging|development')
group by 1,5
order by sid

--1006266380.17005161491701011937
--*/

--1879

--GETFULLCLICKS

/*

with date_range as (
  select
    '20231119' as start_date,
    '20231119' as end_date),
client as (select distinct client_id
                          ,client_src_id
                          ,if(substr(client_name, length(client_url)-3)='',lower(client_url)
                            ,replace(lower(client_url),'www.',concat('www.',substr(client_name, length(client_url)-3),'.'))) as client_url
                            ,vertical
                            ,vertical_region
                            ,client_group
from `isg-dwh-bigquery.dwh.dim_client` 
where company_id=1 and client_url is not null 
     and vertical is not null
     and is_sales_partner=0
     and is_b2b=0),
clicks as (
select distinct
    concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as sid,
    concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value,'.',event_timestamp) as click_request_id,
    format_timestamp('%Y-%m-%d %H:%M:%S',datetime(timestamp_micros(event_timestamp), reporting_timezone)) as times,
    if(instr(device.web_info.hostname,'www.',1)>0,device.web_info.hostname,regexp_extract(device.web_info.hostname,r'\.([a-z]+\.[a-z]{2})$')) as hostname,
    GetParamValue(event_params, 'page_location').string_value as page,
  if(event_name in ('page_view','click'),
              if( instr(GetParamValue(event_params, 'page_location').string_value,'?',1)>0,
              regexp_extract(GetParamValue(event_params, 'page_location').string_value,r'^https?:\/\/(.*)?\?'),
              regexp_extract(GetParamValue(event_params, 'page_location').string_value,r'^https?:\/\/(.*)?')
              ),
              if(event_name='smartfit',lower(GetParamValue(event_params, 'smartfit_action').string_value),
               if(event_name='neocom_conversation',concat('neocom_',lower(GetParamValue(event_params, 'neocom_action').string_value)),
                if(event_name='filter',concat('filter_',lower(GetParamValue(event_params, 'filter_name').string_value)),
                 if(event_name='loqate',concat('loqate_',lower(GetParamValue(event_params, 'loqate_action').string_value)),event_name))))) as click,


             --GetParamValue(event_params, 'kameleoon_segment').string_value as action 
from `isg-dwh-bigquery.analytics_292798251.events_*`,date_range
where _table_suffix between start_date and end_date
and not regexp_contains(device.web_info.hostname,r'staging|development')
and not  regexp_contains(event_name,r'first|start|engage|view_promotion|view_item|view_search|web|items_in_cart|scroll|_loaded|checkout_option|items_in_cart|non-ecommerce|_buffering|_complete|_progress|_seek|kameleoon|view_item_list') 

) 
select sid,click_request_id, regexp_replace(click,r'\/','.') as click,times,client_id
from clicks as cl
left join client as dc on cl.hostname=dc.client_url
where not regexp_contains(click,'modal1_background_fix|loaded|loading')
group by 1,2,3,4,5
;


*/

--/([^\/]+)\/ - ideally takes www.fahrrad.de
--https://(.*\.[a-z]{2,})\



--GETFULLORDERS

/*

with date_range as (
  select
    '20231126' as start_date,
    '20231126' as end_date),
client as (select distinct client_id
                          ,client_src_id
                          ,if(substr(client_name, length(client_url)-3)='',lower(client_url)
                            ,replace(lower(client_url),'www.',concat('www.',substr(client_name, length(client_url)-3),'.'))) as client_url
                            ,vertical
                            ,vertical_region
                            ,client_group
from `isg-dwh-bigquery.dwh.dim_client` 
where company_id=1 and client_url is not null 
     and vertical is not null
     and is_sales_partner=0
     and is_b2b=0)
select
    concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as sid,
    format_timestamp('%Y-%m-%d %H:%M:%S',datetime(timestamp_micros(event_timestamp), reporting_timezone)) as times,
    sum(ecommerce.purchase_revenue) as total_price,
    ecommerce.transaction_id as transaction_id,
    --if(instr(device.web_info.hostname,'www.',1)>0,device.web_info.hostname,regexp_extract(device.web_info.hostname,r'\.([a-z]+\.[a-z]{2})$')) as hostname
    client_id
from `isg-dwh-bigquery.analytics_292798251.events_*` as ga,date_range
left join client as cl 
on if(instr(ga.device.web_info.hostname,'www.',1)>0,ga.device.web_info.hostname,concat('www.',regexp_extract(ga.device.web_info.hostname,r'\.([a-z]+\.[a-z]{2})$')))=cl.client_url
where _table_suffix between start_date and end_date
and not regexp_contains(device.web_info.hostname,r'staging|development')
and ecommerce.transaction_id is not null and ecommerce.transaction_id<>'(not set)'
group by 1,2,4,5
having total_price>0;

--267477953
--292798251

*/


--GETFULLREFERRER

/*

with date_range as (
  select
    '20231126' as start_date,
    '20231126' as end_date),
client as (select distinct client_id
                          ,client_src_id
                          ,if(substr(client_name, length(client_url)-3)='',lower(client_url)
                            ,replace(lower(client_url),'www.',concat('www.',substr(client_name, length(client_url)-3),'.'))) as client_url
                            ,vertical
                            ,vertical_region
                            ,client_group
from `isg-dwh-bigquery.dwh.dim_client` 
where company_id=1 and client_url is not null 
     and vertical is not null
     and is_sales_partner=0
     and is_b2b=0)
select
    concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as sid,
    min(format_timestamp('%Y-%m-%d %H:%M:%S',datetime(timestamp_micros(event_timestamp), reporting_timezone))) as times,
    regexp_extract(GetParamValue(event_params, 'page_referrer').string_value,r'\/([^\/]+)\/') as referrer,
    regexp_extract(GetParamValue(event_params, 'page_referrer').string_value,r'\/\/([^\/]+)') as url,
    SearchEngineAndSocial(regexp_extract(GetParamValue(event_params, 'page_referrer').string_value,r'\/([^\/]+)\/')) as search_engine,
    client_id
from `isg-dwh-bigquery.analytics_292798251.events_*` as ga,date_range
left join client as cl 
on if(instr(ga.device.web_info.hostname,'www.',1)>0,ga.device.web_info.hostname,concat('www.',regexp_extract(ga.device.web_info.hostname,r'\.([a-z]+\.[a-z]{2,})$')))=cl.client_url
where _table_suffix between start_date and end_date
and not regexp_contains(device.web_info.hostname,r'staging|development')
and GetParamValue(event_params, 'page_referrer').string_value is not null
--Here we exclude every referrer where referre contains www.<hostname>.<domain> but we keep hilfe|help subdomains etc.
and regexp_extract(GetParamValue(event_params, 'page_referrer').string_value,r'\/([^\/]+)\/')<>if(instr(device.web_info.hostname,'www.',1)>0,device.web_info.hostname,regexp_extract(device.web_info.hostname,r'\.([a-z]+\.[a-z]{2,})$'))
group by 1,3,4,5,6
;

*/





--GETFULLCUSTOMER
/*

with date_range as (
  select
    '20231119' as start_date,
    '20231119' as end_date),
client as (select distinct client_id
                          ,client_src_id
                          ,if(substr(client_name, length(client_url)-3)='',lower(client_url)
                            ,replace(lower(client_url),'www.',concat('www.',substr(client_name, length(client_url)-3),'.'))) as client_url
                            ,vertical
                            ,vertical_region
                            ,client_group
from `isg-dwh-bigquery.dwh.dim_client` 
where company_id=1 and client_url is not null 
     and vertical is not null
     and is_sales_partner=0
     and is_b2b=0)
select distinct
    user_pseudo_id as eid,
    GetParamValue(user_properties, 'user_id').string_value as customer_id,
    min(format_timestamp('%Y-%m-%d %H:%M:%S',datetime(timestamp_micros(event_timestamp), reporting_timezone))) as times,
    --GetParamValue(user_properties, 'crm_id').string_value as crm_id,
    concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as sid,
    client_id

from `isg-dwh-bigquery.analytics_292798251.events_*` as ga,date_range
left join client as cl 
on if(instr(ga.device.web_info.hostname,'www.',1)>0,ga.device.web_info.hostname,concat('www.',regexp_extract(ga.device.web_info.hostname,r'\.([a-z]+\.[a-z]{2})$')))=cl.client_url
where _table_suffix between start_date and end_date
  and GetParamValue(user_properties, 'user_id').string_value  is not null 
group by 1,2,4,5
order by customer_id desc;


*/
