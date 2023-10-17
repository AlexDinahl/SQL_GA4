--To Get actual data you need to set a timezone as well
--otherwise it will be UTC
  
with date_range as (select format_date('%Y%m%d',date_sub(current_date(), interval 1 day)) as start_date),
data_check as (select exists(select 1 from `isg-dwh-bigquery.analytics_292798251.events_*`,date_range where _table_suffix between start_date and start_date) as condition)
select parse_date('%Y%m%d',event_date) as date_event,* except(start_date,condition)
from `isg-dwh-bigquery.analytics_292798251.events_*`,date_range,data_check
where condition=True and _table_suffix between start_date and start_date and 
extract(hour from timestamp_micros(event_timestamp)) between 19 and 20  
