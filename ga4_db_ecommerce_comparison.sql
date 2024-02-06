create temp function GetParamValue(params any type, target_key string)
as (
  (select `value`from unnest(params) where key = target_key limit 1)
);




with date_range as (
              select
                '20240101' as start_date,
                #FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) as end_date
                '20240121' as end_date
                ),
db_orders as (
select 
                                   order_date_day as order_date
                                   ,order_number as order_id
                                   ,sum(if(total_items is null,0,total_items)) as amount
                                   ,round(sum(if(total_amount is null,0,total_amount)),2) as net_rev
                                   ,round(sum(if(shipment_amount is null,0,shipment_amount)),2) as shipment
                                  from `isg-dwh-bigquery.dwh.fct_order` as fin,date_range
                                  where 
                                  order_date_day between parse_date('%Y%m%d',start_date)
                                   and parse_date('%Y%m%d',end_date)
                                   and client_id=44 and is_test_order=0
                                  group by 1,2
), /*
ua_orders as (

   select            
                      parse_date('%Y%m%d',date) as order_date
                      ,h.transaction.transactionId as order_id
                      ,sum(p.productQuantity) as amount
                      ,round(ifnull(sum(h.transaction.transactionRevenue)/pow(10,6),0),2) as revenue
                    
                      from `isg-dwh-bigquery.206405060.ga_sessions_*`,  UNNEST(hits) as h,unnest(h.product) as p,date_range
                      where _table_suffix between start_date and end_date
                      and h.transaction.transactionId is not null
                 
    group by 1,2

),
*/
ga4_orders as (
select
      cast(event_date as date format 'YYYYMMDD') as order_date,
      ecommerce.transaction_id as order_id,
      if(instr(regexp_extract(GetParamValue(event_params, 'page_referrer').string_value,r'\/\/[^\/]+(\/.*)'),'?',1)>0,
      regexp_extract(GetParamValue(event_params, 'page_referrer').string_value,r'\/\/[^\/]+(\/.*)\?')
      ,regexp_extract(GetParamValue(event_params, 'page_referrer').string_value,r'\/\/[^\/]+(\/.*)')) as referrer,
   regexp_extract(GetParamValue(event_params, 'page_location').string_value,r'\/\/[^\/]+(\/.*)') as page,
      sum(ecommerce.total_item_quantity) as amount,
      sum(ecommerce.purchase_revenue) as revenue,
      sum((select sum(item_revenue) as item_revenue from unnest(items))) as item_revenue,
      sum(ecommerce.tax_value) as tax_value,
      sum(ecommerce.shipping_value) as shipping_value
    from `isg-dwh-bigquery.analytics_292798251.events_*`,date_range 
    where ecommerce.transaction_id  is not null and ecommerce.transaction_id<>'(not set)' --event_name = "purchase" 
    and _table_suffix between start_date and end_date
    group by 1,2,3,4

),
data_loss as (
select 
ifnull(ga4.order_date,db.order_date) as order_date
,ifnull(ga4.order_id,db.order_id) as order_id
,referrer
,page
,ga4.amount as ga4_amount
,db.amount as db_amount
,round(ga4.item_revenue,2) as ga4_revenue
,round(db.net_rev-shipment,2) as db_revenue
from ga4_orders ga4
full outer join db_orders as db using (order_id)
--where ifnull(ga4.order_id,db.order_id)='FDE12064930'
group by 1,2,3,4,5,6,7,8
having sum(ga4_amount) is null and count(referrer)>0
)
select referrer, page, count(distinct order_id) orders_lost
from data_loss
group by 1,2
