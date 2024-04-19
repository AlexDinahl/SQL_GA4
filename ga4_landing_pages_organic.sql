with date_range as (
  select
    '20230831' as start_date,
    '20230901' as end_date),
page as (select
            
       event_date
      ,user_pseudo_id
      ,concat(user_pseudo_id,'_',(select value from unnest(event_params) where key = 'ga_session_id' limit 1).int_value) as ga_session_id
      ,event_name
      
      ,replace(if(instr((select value from unnest(event_params) where key = 'page_location' limit 1).string_value,"?",1, 1)-1<0
                        ,(select value from unnest(event_params) where key = 'page_location' limit 1).string_value
                        ,left((select value from unnest(event_params) where key = 'page_location' limit 1).string_value, instr((select value from unnest(event_params) where key = 'page_location' limit 1).string_value,"?",1, 1)-1)),'https://www.fahrrad.de','') as pagePath
      
      ,(select value from unnest(event_params) where event_name = 'page_view' and key = 'entrances').int_value as is_entrance
      ,ifnull((select value from unnest(event_params) where key = 'medium').string_value,traffic_source.medium)  as traffic_medium
      /*
      ,max(if( (select value.int_value from unnest(event_params) where key = 'entrances') = 1 and 
                  (select value.string_value from unnest(event_params) where event_name = 'page_view' and key = 'medium') is null
                  ,traffic_source.medium
                  ,(select value from unnest(event_params) where key = 'medium').string_value)

      ) as traffic_medium
      */   
    from
      `bigquery.analytics_123456789.events_*`,date_range
      where 
      _table_suffix between start_date and end_date
      and event_name='page_view' 
      and (select value from unnest(event_params) where event_name = 'page_view' and key = 'entrances').int_value>0
      group by 1,2,3,4,5,6,7

),

purchases as (select
      cast(event_date as date format 'YYYYMMDD') as visit_date,
      user_pseudo_id, 
      concat(user_pseudo_id,'_',(select value from unnest(event_params) where key = 'ga_session_id' limit 1).int_value) as ga_session_id,
      ecommerce.transaction_id as transaction_id,
      timestamp_micros(min(event_timestamp)) as session_start_timestamp,
      sum((select sum(item_revenue) as item_revenue from unnest(items))) as item_revenue,
      sum(ecommerce.purchase_revenue) as revenue,
      sum(ecommerce.tax_value) as tax_value,
      sum(ecommerce.shipping_value) as shipping_value
    from `bigquery.analytics_123456789.events_*`,date_range 
    where event_name = "purchase" and _table_suffix between start_date and end_date
    group by 1,2,3,4)

select event_date,pagepath, sum(is_entrance) as entrances,count(distinct transaction_id) as purchases--,sum(revenue) as revenue
from page
left join purchases using (ga_session_id)
where traffic_medium='organic' and event_date between '20230901' and '20230901'
group by 1,2
order by 3 desc
;
