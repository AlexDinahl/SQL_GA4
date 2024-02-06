/*
FOR item IN (
  select table_id
  from `dataset.__TABLES__`
  WHERE STARTS_WITH(table_id, "Table")
)
DO
  EXECUTE IMMEDIATE concat("DROP TABLE `dataset`.",item.table_id);
END FOR;
*/

create temp table client_id as (
  select replace(schema_name,'analytics_','') as property_id,replace(replace(option_value,'GA4',''),'"','') as option_value
from `isg-dwh-bigquery`.`region-EU`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
where regexp_contains(schema_name,'^analytics_')
and option_name='description');

create temp table hostnames (hostname STRING);

FOR item IN (
select property_id
from client_id limit 2
)
DO
  execute immediate concat("insert into hostnames ","select distinct device.web_info.hostname from"," `isg-dwh-bigquery.analytics_",item.property_id,".events_20231005`");
END FOR;

select * from hostnames;
