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
