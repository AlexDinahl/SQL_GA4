with final as (
with date_range as (
  select
    '20230901' as start_date,
    '20231005' as end_date),
  
--FDE
page as (select
            
       event_date
      ,replace(split((select value from unnest(event_params) where key = 'page_location' limit 1).string_value,'/')[safe_offset(2)],'www.','') as shop_name
      ,user_pseudo_id
      ,concat(user_pseudo_id,'_',(select value from unnest(event_params) where key = 'ga_session_id' limit 1).int_value) as ga_session_id
      ,event_name
      ,split(substr((select value from unnest(event_params) where key = 'page_location' limit 1).string_value,
             instr((select value from unnest(event_params) where key = 'page_location' limit 1).string_value,'/',1,3)),'?')[safe_offset(0)] as pagePath
      /*
      ,replace(if(instr((select value from unnest(event_params) where key = 'page_location' limit 1).string_value,"?",1, 1)-1<0
                        ,(select value from unnest(event_params) where key = 'page_location' limit 1).string_value
                        ,left((select value from unnest(event_params) where key = 'page_location' limit 1).string_value, instr((select value from unnest(event_params) where key = 'page_location' limit 1).string_value,"?",1, 1)-1)),'https://www.fahrrad.de','') as pagePath
      */
      ,(select value from unnest(event_params) where event_name = 'page_view' and key = 'breadcrumb').string_value as breadcrumb
      ,(select value from unnest(event_params) where event_name = 'page_view' and key = 'entrances').int_value as is_entrance
     
    from
      `bigquery.analytics_123456789.events_*`,date_range
      where 
      _table_suffix between start_date and end_date
      and event_name='page_view' 
      and (select value from unnest(event_params) where event_name = 'page_view' and key = 'entrances').int_value>0
      group by 1,2,3,4,5,6,7,8

      --CDE
      union all

      (select
            
       event_date
      ,replace(split((select value from unnest(event_params) where key = 'page_location' limit 1).string_value,'/')[safe_offset(2)],'www.','') as shop_name
      ,user_pseudo_id
      ,concat(user_pseudo_id,'_',(select value from unnest(event_params) where key = 'ga_session_id' limit 1).int_value) as ga_session_id
      ,event_name
      ,split(substr((select value from unnest(event_params) where key = 'page_location' limit 1).string_value,
             instr((select value from unnest(event_params) where key = 'page_location' limit 1).string_value,'/',1,3)),'?')[safe_offset(0)] as pagePath
      ,(select value from unnest(event_params) where event_name = 'page_view' and key = 'breadcrumb').string_value as breadcrumb
      ,(select value from unnest(event_params) where event_name = 'page_view' and key = 'entrances').int_value as is_entrance
     
    from
      `bigquery.analytics_123456789.events_*`,date_range
      where 
      _table_suffix between start_date and end_date
      and event_name='page_view' 
      and (select value from unnest(event_params) where event_name = 'page_view' and key = 'entrances').int_value>0
      group by 1,2,3,4,5,6,7,8)

),
purchases as (select
      cast(event_date as date format 'YYYYMMDD') as visit_date
      ,user_pseudo_id
      ,concat(user_pseudo_id,'_',(select value from unnest(event_params) where key = 'ga_session_id' limit 1).int_value) as ga_session_id
      ,ecommerce.transaction_id as transaction_id
      ,timestamp_micros(min(event_timestamp)) as session_start_timestamp
      ,sum((select sum(item_revenue) as item_revenue from unnest(items))) as item_revenue
      ,sum(ecommerce.purchase_revenue) as revenue
      ,sum(ecommerce.tax_value) as tax_value
      ,sum(ecommerce.shipping_value) as shipping_value
    from `bigquery.analytics_123456789.events_*`,date_range 
    where event_name = "purchase" and _table_suffix between start_date and end_date
    group by 1,2,3,4
    
    --CDE
    union all (

      select
      cast(event_date as date format 'YYYYMMDD') as visit_date
      ,user_pseudo_id
      ,concat(user_pseudo_id,'_',(select value from unnest(event_params) where key = 'ga_session_id' limit 1).int_value) as ga_session_id
      ,ecommerce.transaction_id as transaction_id
      ,timestamp_micros(min(event_timestamp)) as session_start_timestamp
      ,sum((select sum(item_revenue) as item_revenue from unnest(items))) as item_revenue
      ,sum(ecommerce.purchase_revenue) as revenue
      ,sum(ecommerce.tax_value) as tax_value
      ,sum(ecommerce.shipping_value) as shipping_value
    from `bigquery.analytics_123456789.events_*`,date_range 
    where event_name = "purchase" and _table_suffix between start_date and end_date
    group by 1,2,3,4

    )  
    
    )

select parse_date('%Y%m%d',event_date) as event_date
,shop_name
--,pagepath
,regexp_extract(breadcrumb, r'^(?:[^>]+>){1}([^>]+)') as level1
,regexp_extract(breadcrumb, r'^(?:[^>]+>){2}([^>]+)') as level2
,regexp_extract(breadcrumb, r'^(?:[^>]+>){3}([^>]+)') as level3
,regexp_extract(breadcrumb, r'^(?:[^>]+>){4}([^>]+)') as level4
, sum(is_entrance) as entrances,count(distinct transaction_id) as purchases--,sum(revenue) as revenue
from page
left join purchases using (ga_session_id)
where not regexp_contains(shop_name,r'staging|development|dev|salesforce|demandware|translate|appspot|internetstores|shop')
group by 1,2,3,4,5,6
--having count(distinct transaction_id)>0
--order by shop_name,purchases desc
)
select event_date
      ,shop_name
      --,pagepath
      ,dense_rank() over (partition by shop_name order by purchases desc) as rnk
      ,level1
      ,level2
      ,level3
      ,level4
      ,entrances
      ,purchases

from final
;
