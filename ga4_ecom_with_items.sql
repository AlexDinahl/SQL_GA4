select distinct
event_date
,user_pseudo_id
,ecommerce.transaction_id
,i.item_id
,i.item_variant
,i.item_brand
,i.item_name
,i.item_category
,case when e.key='coupon_discount' then if(null,0,e.value.int_value) else 0 end as coupon_discount
,i.coupon
,case when e.key='discount' then if(null,0,e.value.int_value) else 0 end as discount
,case when e.key='payback_points' then if(null,0,e.value.int_value) else 0 end as payback_points
,ecommerce.total_item_quantity as total_item_quantity
,ecommerce.purchase_revenue  as purchase_revenue
,ecommerce.shipping_value  as shipping_value
,ecommerce.tax_value  as tax_value
,i.quantity as item_quantity
,case when e.key='item_original_price' then if(null,0,e.value.int_value) else 0 end as item_original_price
,i.price as item_price
,i.item_revenue as item_revenue
from 
`bigquery.analytics_123456789.events_*`,unnest(event_params) as e, unnest(items) as i
where _table_suffix between '20230101' and '20230111' and event_name='purchase' and ecommerce.transaction_id is not null
;
