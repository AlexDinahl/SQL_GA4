with date_range as (
  select
    '20231003' as start_date,
    '20231003' as end_date),
conversions as
  ( 
    select
      cast(event_date as date format 'YYYYMMDD') as visit_date,
      user_pseudo_id, 
      concat(user_pseudo_id,'_',(select value from unnest(event_params) where key = 'ga_session_id' limit 1).int_value) as ga_session_id,
      ecommerce.transaction_id as transaction_id,
      timestamp_micros(min(event_timestamp)) as session_start_timestamp,
      sum((select sum(item_revenue) as item_revenue from unnest(items))) as item_revenue,
      sum(ecommerce.purchase_revenue) as revenue,
      sum(ecommerce.tax_value) as tax_value,
      sum(ecommerce.shipping_value) as shipping_value
    from `isg-dwh-bigquery.analytics_292798251.events_*`,date_range 
    where event_name = "purchase" and _table_suffix between start_date and end_date
    group by 1,2,3,4
  )
select visit_date,transaction_id as order_id
      ,round(sum(revenue),2) as revenue
      ,round(sum(tax_value),2) as tax_value
      ,round((sum(revenue)+sum(tax_value))-(sum(shipping_value)/1.19),2) as revenue_with_tax_minus_shipping
      ,round(sum(shipping_value),2) as shipping
      ,round(sum(item_revenue),2) as item_revenue
from conversions
group by 1,2
