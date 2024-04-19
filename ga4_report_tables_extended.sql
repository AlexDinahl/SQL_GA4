declare reporting_timezone string default 'Europe/Berlin';

create temp function GetParamValue(params any type, target_key string)
as (
  (select `value`from unnest(params) where key = target_key limit 1)
);

create temp function ChannelGrouping(csource string,medium string,campaign string,gclid string) as (
    case
        when medium='organic' or regexp_contains(csource, r'^(.*search\.yahoo.*|msn|yandex|.*lens\.google.*)$')  then 'Organic Search'
        when (csource = 'direct' or csource is null) 
            and (regexp_contains(medium, r'^(\(not set\)|\(none\))$') or medium is null) 
            then 'Direct'
        when regexp_contains(csource, r'^(google|bing)$') 
            and regexp_contains(medium, r'^(cp.*|ppc)$') --or gclid is not null
            then 'SEA'
        when  medium='psm' or regexp_contains(csource,r'mydealz') 
            then 'Price Comparison'
        when  medium='social' then 'Social Paid'
        when  medium='post' or regexp_contains(csource, r'^.*(twitter|t\.co|facebook|instagram|linkedin|lnkd\.in|pinterest).*') then 'Social'
        when regexp_contains(medium,r'display|video') 
            then 'Display'
        when medium='email' or (regexp_contains(csource,r'.*mail\.|.*deref\-')) then 'CRM'
        when  regexp_contains(campaign,r'rmkt')  
            or (regexp_contains(medium,r'rmkt')) then 'Remarketing'
        when  regexp_contains(medium,r'[Cc]oop') then 'Cooperation'
        when  medium='voucher' 
            then 'Voucher'
        when medium='referral' 
            then 'Referral'
        when  medium='affiliate' then 'Affiliate'
        else '(Other)'
    end
);




/*
create temp table client_id as (
  SELECT if(replace(lower(client_name),lower(replace(client_url,'www.','')),'')='fr'
  ,concat('fr','.',lower(replace(client_url,'www.','')))
  ,lower(replace(client_url,'www.',''))) as website
  ,client_src_id
  ,client_id --distinct client_id,lower(replace(client_url,'www.','')) as url
  FROM `bigquery.dwh.dim_client` 
  where company_code='ISG' and vertical<>'MARKETPLACE' 
  and is_b2b=0 and business_unit is not null and client_src_id<>'BUN');
*/

--Traffic Overview
--Best so far

/*
with date_range as (
  select
    '20231025' as start_date,
    '20231025' as end_date),
sessions as ( select
                        date(datetime(timestamp_micros(event_timestamp), reporting_timezone)) as visit_date,
                        case
                          when GetParamValue(event_params, 'ga_session_number').int_value=1 and event_name = 'session_start'  then 'new visitor'
                          when GetParamValue(event_params, 'ga_session_number').int_value>1 and event_name = 'session_start'
                                                                                                then 'returning visitor'
        else null end as user_type,
                        GetParamValue(event_params, 'entrances').int_value as is_entrance,
                        GetParamValue(event_params, 'session_engaged').int_value as engaged_session,
                        user_pseudo_id,
                        if(event_name = 'page_view',1,0) as pageviews,
                        if(event_name='add_to_cart',1,0) as adds_to_cart,
                        concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as session_id
from `bigquery.analytics_123456789.events_*`,date_range
where _table_suffix
between start_date and end_date
),
iov as (
  select order_date_day as visit_date
        ,count(distinct order_number) as orders
        ,sum(iov) as order_value
from `bigquery.dwh.iov_products`,date_range
where order_date_day
between parse_date('%Y%m%d',start_date) and parse_date('%Y%m%d',start_date)
and client_id=44
group by 1
)
select visit_date,
       count(distinct session_id) as session_count
       ,count(distinct user_pseudo_id) as visitors
       ,count(distinct if(user_type='new visitor',user_pseudo_id,null)) as new_visitors
       ,sum(pageviews) as page_impressions
       ,sum(is_entrance) as entries
       ,count(distinct session_id) - sum(engaged_session) as bounces
       ,orders as order_count
       ,sum(adds_to_cart) as basket
       ,order_value as order_value
from sessions
left join iov using(visit_date)
group by 1,8,10;

*/




/*
with date_range as (
  select
    '20231005' as start_date,
    '20231005' as end_date)
select
  date(datetime(timestamp_micros(event_timestamp), reporting_timezone)) as visit_date,
  'GA' as source,
 count( distinct
      concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value)) AS session_count
from `bigquery.analytics_123456789.events_*`,date_range
where _table_suffix between start_date  and end_date
group by 1,2;

*/




--V_VISIT_DURATION
/*
with date_range as (
  select
    '20231005' as start_date,
    '20231005' as end_date),
events as (
  select
    date(datetime(timestamp_micros(event_timestamp), reporting_timezone)) as visit_date,
    user_pseudo_id,
    --device.web_info.hostname,
    concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as session_id,
    GetParamValue(event_params, 'ga_session_id').int_value as start_time,
    cast(min(event_timestamp)/pow(10,6) as int64) as event_start_time,
    cast(max(event_timestamp)/pow(10,6) as int64) as end_time,
    
  from  `bigquery.analytics_123456789.events_*`,date_range
  where _table_suffix between start_date  and end_date 
  --and device.web_info.hostname in ('www.fahrrad.de','hilfe.fahrrad.de')
  group by 1,2,3,4
  having count(distinct GetParamValue(event_params, 'entrances').int_value)>0
)
  select
   session_id,
   visit_date,
   timestamp(datetime(timestamp_seconds(if(start_time>end_time,event_start_time,start_time))),reporting_timezone) as visit_start_time,
   timestamp(datetime(timestamp_seconds(end_time)),reporting_timezone) as visit_end_time,
   extract(month from visit_date) as visit_month,
  -- format_timestamp('%M:%S',timestamp_seconds(cast(round(safe_divide(sum(end_time-start_time),count(distinct session_id)),0) as int64))) as duration_time,
  cast(round(safe_divide(sum(if(end_time>start_time,end_time-start_time,end_time-event_start_time)),hll_count.extract(hll_count.init(session_id,12))),0) as int64) as duration_sec
from events
group by 1,2,3,4,5
*/

--V_VISITS_BY_CLIENT

/*
with date_range as (
  select
    '20231005' as start_date,
    '20231005' as end_date),
events as (
  select
    date(datetime(timestamp_micros(event_timestamp), reporting_timezone)) as visit_date,
    user_pseudo_id,
    --device.web_info.hostname,
    concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as session_id,
    device.category as device_category
  from  `bigquery.analytics_123456789.events_*`,date_range
  where _table_suffix between start_date  and end_date 
  --and device.web_info.hostname in ('www.fahrrad.de','hilfe.fahrrad.de')
  group by 1,2,3,4
)
  select
  visit_date,
  count(distinct session_id) as visits,
  --hll_count.extract(hll_count.init(if(device_category='desktop',session_id,null),12)) 
  count(distinct if(device_category='desktop',session_id,null)) as desktop,
  --hll_count.extract(hll_count.init(if(device_category='mobile',session_id,null),12)) 
  count(distinct if(device_category='mobile',session_id,null))as mobile,
  --hll_count.extract(hll_count.init(if(device_category='tablet',session_id,null),12)) 
  count(distinct if(device_category='tablet',session_id,null)) as tablet,
  --hll_count.extract(hll_count.init(if(device_category='smart tv',session_id,null),12)) 
  count(distinct if(device_category='smart tv',session_id,null)) as tv,
  --hll_count.extract(hll_count.init(if(device_category not in ('desktop','mobile','tablet','smart tv'),session_id,null),12)) 
  count(distinct if(device_category not in ('desktop','mobile','tablet','smart tv'),session_id,null)) as other
from events
group by 1
*/


/*
--V_VISITS_BY_GEO

--Some cities contain plz instead of city name. It is inside the big query itself
--What do we do with this data?

with date_range as (
  select
    '20231005' as start_date,
    '20231005' as end_date),
events as (
  select
    date(datetime(timestamp_micros(event_timestamp), reporting_timezone)) as visit_date,
    user_pseudo_id,
    if(geo.country='','unknown',geo.country) as country,
    if(geo.region='','unknown',geo.region)  as region,
    if(geo.city='','unknown',geo.city) as city,
    concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as session_id,
  from  `bigquery.analytics_123456789.events_*`,date_range
  where _table_suffix between start_date  and end_date
group by 1,2,3,4,5,6
)
  select
  visit_date,
  country,
  region,
  city,
  count(distinct session_id) as visits
from events
group by 1,2,3,4
*/


--V_PRODUCT_VISITS


--VISIT_DATE,PRODUCT_ID,PRODUCT_SRC_ID,ARTICLE_ID,ARTICLE_SRC_ID,NET_SALES_PRICE,TOTAL_PRODUCT_VISIT,TOTAL_PRODUCT_ORDERED,TOTAL_PRODUCT_VIEWED,ADDED_TO_BASKET,INSERT_TIMESTAMP,UPDATE_TIMESTAMP



/*
with date_range as (
  select
    '20231025' as start_date,
    '20231025' as end_date),
products as (select distinct 
               product_id
              ,product_src_id
              ,article_id
              ,cast(article_src_id as numeric) as article_src_id
              from `bigquery.dwh.product_details` 
              where company_id=1 ),
items as  (
        select 
              date(datetime(timestamp_micros(event_timestamp), reporting_timezone)) as visit_date,

              items.item_id as product_src_id, 
              GetParamValue(item_params, 'item_variation').int_value as article_src_id,
              ecommerce.transaction_id as order_number,
              sum(if(event_name='view_item_list',1,0)) as item_list_views, 
              sum(if(event_name='select_item',1,0)) as item_list_clicks,
              sum(if(event_name='view_item',1,0)) as item_pdp_views,
              sum(if(event_name='add_to_cart',1,0)) as item_adds_to_cart,
              --sum(if(ecommerce.transaction_id<>'(not set)',1,0)) as purchases,
              sum(if(ecommerce.transaction_id<>'(not set)',items.quantity,0)) as item_quantity_purchased,
             -- round(sum(ifnull(item_revenue,0)),2) as item_revenue

 from `bigquery.analytics_123456789.events_*` ,date_range,unnest(items) as items
 where items.item_id<>'(not set)' and _table_suffix between start_date and end_date
    and event_name not like '%_promotion'
group by 1,2,3,4
having item_quantity_purchased+item_pdp_views+item_adds_to_cart>0
),
iov_products as (
  select order_date_day as visit_date,
                        order_number,
                        product_id,
                        product_src_id,
                        article_id,
                        cast(article_src_id as numeric) as article_src_id,
                        quantity_ordered
                        ,iov as net_sales_price
                        ,row_number() over (partition by order_date_day,product_id,product_src_id,article_id order by order_number) as rn
from `bigquery.dwh.iov_products`,date_range
where order_date_day
between parse_date('%Y%m%d',start_date) and parse_date('%Y%m%d',end_date)
and client_id=44)
select 
ifnull(fin.visit_date,itm.visit_date) as visit_date
,ifnull(fin.product_id,ifnull(pd.product_id,pdc.product_id)) as product_id
,ifnull(fin.product_src_id,itm.product_src_id) as product_src_id
,ifnull(fin.article_id ,pdc.article_id) as article_id
,ifnull(fin.article_src_id,itm.article_src_id) as article_src_id
--,ifnull(fin.net_sales_price,0) as net_sales_price
,ifnull(sum(fin.quantity_ordered),0) as total_product_ordered
,ifnull(sum(item_pdp_views),0) as total_product_viewed
,ifnull(sum(item_adds_to_cart),0) as added_to_basket
from  iov_products as fin
full outer join items as itm using(order_number,product_src_id,article_src_id)
left join products as pd on pd.product_src_id=itm.product_src_id and itm.article_src_id is null
left join products as pdc on pdc.product_src_id=itm.product_src_id and pdc.article_src_id=itm.article_src_id and itm.article_src_id is not null
group by 1,2,3,4,5
*/

--if we add net_sales_price we get more rows 822 instead of 805




--V_PRODUCT_ORDER_DEVICETYPE

--PRODUCT_ID	ARTICLE_ID	CLIENT_SRC_ID	VISIT_ID	VISIT_DATE	NET_SALES_PRICE	ORDERS	PRODUCT_VIEWS	ADDED_TO_BASKET	DESKTOP	MOBILE	TABLET	CONSOLE	TV	OTHER

/*

with date_range as (
  select
    '20231025' as start_date,
    '20231025' as end_date),
products as (select distinct product_id,article_id,product_src_id,cast(article_src_id as numeric) as article_src_id
from `bigquery.dwh.product_details`
where company_id=1),
client as (select distinct client_id
                          ,client_src_id
                          ,if(substr(client_name, length(client_url)-3)='',lower(client_url)
                            ,replace(lower(client_url),'www.',concat('www.',substr(client_name, length(client_url)-3),'.'))) as client_url
                            ,vertical
                            ,vertical_region
                            ,client_group
from `bigquery.dwh.dim_client` 
where company_id=1 and client_url is not null 
     and vertical is not null
     and is_sales_partner=0
     and is_b2b=0),
items as  (
        select
              date(datetime(timestamp_micros(event_timestamp), reporting_timezone)) as visit_date,
              if(instr(device.web_info.hostname,'www.',1)>0,device.web_info.hostname,regexp_extract(device.web_info.hostname,r'\.([a-z]+\.[a-z]{2})$')) as hostname,
              concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as session_id,
              items.item_id as product_src_id,
              GetParamValue(item_params, 'item_variation').int_value as article_src_id,
              ecommerce.transaction_id as order_number,
              device.category as device_category,
              sum(if(event_name='view_item',1,0)) as item_pdp_views,
              sum(if(event_name='add_to_cart',1,0)) as item_adds_to_cart,
              sum(if(ecommerce.transaction_id<>'(not set)',1,0)) as purchases,
              sum(if(ecommerce.transaction_id<>'(not set)',items.quantity,0)) as item_quantity_purchased,
              round(sum(ifnull(item_revenue,0)),2) as item_revenue
 
 from `bigquery.analytics_123456789.events_*` ,date_range,unnest(items) as items
 where items.item_id<>'(not set)' and _table_suffix between start_date and end_date
       and event_name not like '%_promotion'
group by 1,2,3,4,5,6,7
having purchases+item_pdp_views+item_adds_to_cart>0
),
iov_products as (
  select order_date_day as visit_date,
                        client_id,
                        order_number,
                        product_id,
                        product_src_id,
                        article_id,
                        cast(article_src_id as numeric) as article_src_id,
                        quantity_ordered
                        ,iov as net_sales_price
                        ,row_number() over (partition by order_date_day,product_id,product_src_id,article_id order by order_number) as rn
from `bigquery.dwh.iov_products`,date_range
where order_date_day
between parse_date('%Y%m%d',start_date) and parse_date('%Y%m%d',end_date)
and client_id=44)
select
 
 ifnull(fin.product_id,ifnull(pd.product_id,pdc.product_id)) as product_id
,ifnull(fin.article_id,pdc.article_id) as article_id
,client_src_id
,ifnull(session_id,'-1') as session_id
,ifnull(fin.visit_date,itm.visit_date) as visit_date
--,ifnull(round(avg(fin.net_sales_price),2),0) as net_sales_price
,count(distinct fin.order_number) as orders
,ifnull(sum(item_pdp_views),0) as product_views
,ifnull(sum(item_adds_to_cart),0) as added_to_basket
,if(device_category='desktop',1,0) as desktop
,if(device_category='mobile',1,0) as mobile
,if(device_category='tablet',1,0) as tablet
,if(device_category='smart tv',1,0) as tv
,if(device_category not in ('desktop','mobile','tablet','smart tv'),1,0) as other
from  iov_products as fin
full outer join items as itm using(order_number,product_src_id,article_src_id)
left join products as pd on pd.product_src_id=itm.product_src_id and itm.article_src_id is null
left join products as pdc on pdc.product_src_id=itm.product_src_id and pdc.article_src_id=itm.article_src_id and itm.article_src_id is not null
left join client as dc on case when fin.client_id is null then itm.hostname=dc.client_url else fin.client_id=dc.client_id end
group by 1,2,3,4,5,9,10,11,12,13

;
*/







--V_PRODUCT_ORDERS


--ORDER_DATE	PRODUCT_NAME	LEVEL0	LEVEL1	LEVEL2	LEVEL3	LEVEL4	CLIENT_SRC_ID	BRAND_NAME	VERTICAL	VERTICAL_REGION	CLIENT_GROUP	LANGUAGE_NAME	COUNTRY_NAME	PRODUCT_ID	PRODUCT_SRC_ID	PRICE_RANGE	ORDERS	PRODUCT_VIEWS	ORDERS_AVG	PRODUCT_VIEWS_AVG	STOCK_LEVEL	IOV	IOV_GP

/*

with date_range as (
  select
    '20231025' as start_date,
    '20231025' as end_date),
products as (select distinct product_id,product_src_id,product_name,brand_name,level1,level2,level3,level4
from `bigquery.dwh.product_details`
where company_id=1),
client as (select distinct client_id
                          ,if(substr(client_name, length(client_url)-3)='',lower(client_url)
                            ,replace(lower(client_url),'www.',concat('www.',substr(client_name, length(client_url)-3),'.'))) as client_url
                            ,vertical
                            ,vertical_region
                            ,client_group
from `bigquery.dwh.dim_client` 
where company_id=1 and client_url is not null 
     and vertical is not null
     and is_sales_partner=0
     and is_b2b=0),
items as  (
        select 
              date(datetime(timestamp_micros(event_timestamp), reporting_timezone)) as visit_date,
              if(instr(device.web_info.hostname,'www.',1)>0,device.web_info.hostname,regexp_extract(device.web_info.hostname,r'\.([a-z]+\.[a-z]{2})$')) as hostname,
              items.item_id as product_src_id, 
              --GetParamValue(item_params, 'item_variation').int_value as article_src_id,
              --ecommerce.transaction_id as order_number,
              --device.category as device_category,
              sum(if(event_name='view_item',1,0)) as item_pdp_views,
              sum(if(event_name='add_to_cart',1,0)) as item_adds_to_cart,
              sum(if(ecommerce.transaction_id<>'(not set)',1,0)) as purchases,
              --sum(if(ecommerce.transaction_id<>'(not set)',items.quantity,0)) as item_quantity_purchased,
              --round(sum(ifnull(item_revenue,0)),2) as item_revenue

 from `bigquery.analytics_123456789.events_*` ,date_range,unnest(items) as items
 where items.item_id<>'(not set)' and _table_suffix between start_date and end_date
       and event_name not like '%_promotion' and instr(device.web_info.hostname,'www.',1)>0
group by 1,2,3
having purchases+item_pdp_views+item_adds_to_cart>0
),
iov_products as (
  select order_date_day as visit_date,
                        client_id,
                        order_number,
                        product_id,
                        product_src_id,
                        article_id,
                        cast(article_src_id as numeric) as article_src_id,
                        quantity_ordered,
                        iov,
                        iov_gp,
from `bigquery.dwh.iov_products`,date_range
where order_date_day
between parse_date('%Y%m%d',start_date) and parse_date('%Y%m%d',end_date)
and client_id=44),
products_total as (
        select 
        ifnull(fin.visit_date,itm.visit_date) as order_date
        ,ifnull(fin.product_src_id,itm.product_src_id) as product_src_id
        ,client_id
        ,hostname
        ,ifnull(sum(fin.quantity_ordered),0) as orders
        ,ifnull(sum(item_pdp_views),0) as product_views
        ,ifnull(sum(iov),0) as iov
        ,ifnull(sum(iov_gp),0) as iov_gp
        from  iov_products as fin
        full outer join items as itm using(product_src_id)
        group by 1,2,3,4
)
select pt.order_date
      ,pd.product_name
      ,ifnull(level4,level3) as level0
      ,level1
      ,level2
      ,level3
      ,level4
      ,pt.product_src_id
      ,brand_name
      ,vertical
      ,vertical_region
      ,client_group
      ,orders
      ,product_views
      ,iov
      ,iov_gp
from products_total as pt
left join products as pd on pt.product_src_id=pd.product_src_id
left join client as dc on case when pt.client_id is null then pt.hostname=dc.client_url else pt.client_id=dc.client_id end
where orders>0
;

*/



---V_PRODUCT_REVENUE_DEVICETYPE 

--ORDER_DATE	PRODUCT_ID	CLIENT_ID	SHOP	PRODUCT_NAME	LEVEL1	LEVEL2	LEVEL3	LEVEL4	BRAND_NAME	VERTICAL	ORDERS_DESKTOP	ORDERS_MOBILE	ORDERS_TABLET	ORDERS_CONSOLE	ORDERS_TV	ORDERS_OTHER	VIEWS_DESKTOP	VIEWS_MOBILE	VIEWS_TABLET	VIEWS_CONSOLE	VIEWS_TV	VIEWS_OTHER	REVENUE_DESKTOP	REVENUE_MOBILE	REVENUE_TABLET	REVENUE_CONSOLE	REVENUE_TV	REVENUE_OTHER	GP_DESKTOP	GP_MOBILE	GP_TABLET	GP_CONSOLE	GP_TV	GP_OTHER


/*
with tot as  (
with date_range as (
  select
    '20231119' as start_date,
    '20231119' as end_date),
products as (select distinct product_id,product_name,article_id,level1,level2,level3,level4,brand_name
                    ,product_src_id,cast(article_src_id as numeric) as article_src_id
from `bigquery.dwh.product_details`
where company_id=1),
client as (select distinct client_id
                          ,client_src_id
                          ,if(substr(client_name, length(client_url)-3)='',lower(client_url)
                            ,replace(lower(client_url),'www.',concat('www.',substr(client_name, length(client_url)-3),'.'))) as client_url
                          ,vertical
                          ,vertical_region
                          ,client_group
from `bigquery.dwh.dim_client`
where company_id=1 and client_url is not null
     and vertical is not null
     and is_sales_partner=0
     and is_b2b=0),
items as  (
        select
              date(datetime(timestamp_micros(event_timestamp), reporting_timezone)) as visit_date,
              if(instr(device.web_info.hostname,'www.',1)>0,device.web_info.hostname,regexp_extract(device.web_info.hostname,r'\.([a-z]+\.[a-z]{2})$')) as hostname,
              --concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value) as session_id,
              items.item_id as product_src_id,
              GetParamValue(item_params, 'item_variation').int_value as article_src_id,
              ecommerce.transaction_id as order_number,
              device.category as device_category,
              sum(if(event_name='view_item',1,0)) as item_pdp_views,
              sum(if(event_name='add_to_cart',1,0)) as item_adds_to_cart,
              sum(if(ecommerce.transaction_id<>'(not set)',1,0)) as purchases,
              sum(if(ecommerce.transaction_id<>'(not set)',items.quantity,0)) as item_quantity_purchased,
              round(sum(ifnull(item_revenue,0)),2) as item_revenue
  
 from `bigquery.analytics_123456789.events_*` ,date_range,unnest(items) as items
 where items.item_id<>'(not set)' and _table_suffix between start_date and end_date
       and event_name not like '%_promotion'
group by 1,2,3,4,5,6
having purchases+item_pdp_views>0
),
iov_products as (
  select order_date_day as visit_date,
                        client_id,
                        order_number,
                        product_id,
                        product_src_id,
                        article_id,
                        cast(article_src_id as numeric) as article_src_id,
                        quantity_ordered
                        ,iov
                        ,iov_gp
from `bigquery.dwh.iov_products`,date_range
where order_date_day
between parse_date('%Y%m%d',start_date) and parse_date('%Y%m%d',end_date)
and client_id=44),
products_total as (
        select
         
        ifnull(fin.visit_date,itm.visit_date) as order_date
        ,client_id
        ,hostname
        ,product_id
        ,ifnull(fin.product_src_id,itm.product_src_id) as product_src_id
        ,ifnull(fin.article_src_id,itm.article_src_id) as article_src_id
        ,ifnull(count(distinct if(device_category='desktop',fin.order_number,null)),0) as orders_desktop
        ,ifnull(count(distinct if(device_category='mobile',fin.order_number,null)),0) as orders_mobile
        ,ifnull(count(distinct if(device_category='tablet',fin.order_number,null)),0) as orders_tablet
        ,ifnull(count(distinct if(device_category='smart tv',fin.order_number,null)),0) as orders_tv
        ,ifnull(count(distinct if(device_category not in ('desktop','mobile','tablet','smart tv'),order_number,null)),0) as orders_other
        ,ifnull(count(distinct if(device_category is null,order_number,null)),0) as orders_device_undefined
        ,ifnull(sum(if(device_category='desktop',item_pdp_views,null)),0) as views_desktop
        ,ifnull(sum(if(device_category='mobile',item_pdp_views,null)),0) as views_mobile
        ,ifnull(sum(if(device_category='tablet',item_pdp_views,null)),0) as views_tablet
        ,ifnull(sum(if(device_category='smart tv',item_pdp_views,null)),0) as views_tv
        ,ifnull(sum(if(device_category not in ('desktop','mobile','tablet','smart tv'),item_pdp_views,null)),0) as views_other
        ,ifnull(sum(if(device_category='desktop',iov,null)),0) as revenue_desktop
        ,ifnull(sum(if(device_category='mobile',iov,null)),0) as revenue_mobile
        ,ifnull(sum(if(device_category='tablet',iov,null)),0) as revenue_tablet
        ,ifnull(sum(if(device_category='smart tv',iov,null)),0) as revenue_tv
        ,ifnull(sum(if(device_category not in ('desktop','mobile','tablet','smart tv'),iov,null)),0) as revenue_other
        ,ifnull(sum(if(device_category is null,iov,null)),0) as revenue_device_undefined
        ,ifnull(sum(if(device_category='desktop',iov_gp,null)),0) as gp_desktop
        ,ifnull(sum(if(device_category='mobile',iov_gp,null)),0) as gp_mobile
        ,ifnull(sum(if(device_category='tablet',iov_gp,null)),0) as gp_tablet
        ,ifnull(sum(if(device_category='smart tv',iov_gp,null)),0) as gp_tv
        ,ifnull(sum(if(device_category not in ('desktop','mobile','tablet','smart tv'),iov_gp,null)),0) as gp_other
        ,ifnull(sum(if(device_category is null,iov_gp,null)),0) as gp_device_undefined
        from  iov_products as fin
        full outer join items as itm using(order_number,product_src_id,article_src_id)
        group by 1,2,3,4,5,6
)
select order_date
      ,ifnull(pt.product_id,ifnull(pd.product_id,pdc.product_id)) as product_id
      ,dc.client_id
      ,client_src_id as shop
      ,ifnull(pd.product_name,pdc.product_name) as product_name
      ,ifnull(pd.level1,pdc.level1) as level1
      ,ifnull(pd.level2,pdc.level2) as level2
      ,ifnull(pd.level3,pdc.level3) as level3
      ,ifnull(pd.level4,pdc.level4) as level4
      ,ifnull(pd.brand_name,pdc.brand_name) as brand_name
      ,vertical
      ,orders_desktop
      ,orders_mobile
      ,orders_tablet
      ,orders_tv
      ,orders_other
      ,orders_device_undefined
      ,views_desktop
      ,views_mobile
      ,views_tablet
      ,views_tv
      ,views_other
      ,revenue_desktop
      ,revenue_mobile
      ,revenue_tablet
      ,revenue_tv
      ,revenue_other
      ,revenue_device_undefined
      ,gp_desktop
      ,gp_mobile
      ,gp_tablet
      ,gp_tv
      ,gp_other
      ,gp_device_undefined
from products_total as pt
left join products as pd on pd.product_src_id=pt.product_src_id and pt.article_src_id is null
left join products as pdc on pdc.product_src_id=pt.product_src_id and pdc.article_src_id=pt.article_src_id and pt.article_src_id is not null
left join client as dc on case when pt.client_id is null then pt.hostname=dc.client_url else pt.client_id=dc.client_id end
--where pt.product_id=1015054 ---pd.product_name is null
)
select order_date 
      ,sum(orders_desktop+orders_mobile+orders_tablet+orders_tv+orders_other+orders_device_undefined) as ord
      ,sum(views_desktop+views_mobile+views_tablet+views_tv+views_other) as views
      ,sum(revenue_desktop+revenue_mobile+revenue_tablet+revenue_tv+revenue_other+revenue_device_undefined) as rev
      ,sum(gp_desktop+gp_mobile+gp_tablet+gp_tv+gp_other+gp_device_undefined) as gp
from tot
group by 1
;

*/


--V_MARKETING_ONLINE_VISIT_DAILY (does not wor without attribution)

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
            from `bigquery.dwh.dim_client`
            where company_id=1 and client_url is not null
                  and vertical is not null
                  and is_sales_partner=0
                  and is_b2b=0)
        select
              date(datetime(timestamp_micros(event_timestamp), reporting_timezone)) as visit_date,
              --if(instr(device.web_info.hostname,'www.',1)>0,device.web_info.hostname,regexp_extract(device.web_info.hostname,r'\.([a-z]+\.[a-z]{2})$')) as hostname,
              ChannelGrouping(collected_traffic_source.manual_source,collected_traffic_source.manual_medium
                              ,collected_traffic_source.manual_campaign_name,collected_traffic_source.gclid) as channel_grouping,
              --collected_traffic_source.manual_medium as medium,
              --collected_traffic_source.manual_campaign_name as campaign,
              --collected_traffic_source.gclid as gclid,
              count(distinct concat(user_pseudo_id,GetParamValue(event_params, 'ga_session_id').int_value)) visit_cnt
  
 from `bigquery.analytics_123456789.events_*` ,date_range
 where _table_suffix between start_date and end_date and device.web_info.hostname like ('%fahrrad%')
group by 1,2
