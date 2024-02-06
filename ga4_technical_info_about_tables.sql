--SELECT * FROM `isg-dwh-bigquery.analytics_292798251.INFORMATION_SCHEMA.TABLES`;
/*
SELECT *, DATETIME(TIMESTAMP_MILLIS(last_modified_time),'Europe/Berlin') as last_modified
FROM `analytics_292798251.__TABLES__`
where table_id LIKE 'events_%';
*/



select replace(schema_name,'analytics_','') as property_id,replace(replace(option_value,'GA4',''),'"','') as option_value
from `isg-dwh-bigquery`.`region-EU`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
where regexp_contains(schema_name,'^analytics_')
and option_name='description';






select replace(schema_name,'analytics_','') as schema_name, if(regexp_contains(schema_name,'^analytics_'),replace(key,'ga_','ga4_'),key) as label_key
, value as label_value
from
(
 select schema_name, option_name, option_value,
  array
  (
   select as struct arr[offset(0)] key , arr[offset(1)] value 
   from unnest(regexp_extract_all(option_value, r'STRUCT\(("[^"]+", "[^"]+")\)')) kv, 
    unnest([struct(split(replace(kv, '"', ''), ', ') as arr)])
  ) as labels
 from `isg-dwh-bigquery`.`region-EU`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
 where option_name in ('labels')
),
unnest(labels)
--where key='ga_analytics_view'
order by 2, 3;
