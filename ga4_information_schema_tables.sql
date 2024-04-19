--GA4 and UA tables

SELECT
  REPLACE(schema_name,'analytics_','') AS schema_name,
IF
  (REGEXP_CONTAINS(schema_name,'^analytics_'),REPLACE(KEY,'ga_','ga4_'),KEY) AS label_key,
  value AS label_value
FROM (
  SELECT
    schema_name,
    option_name,
    option_value,
    ARRAY (
    SELECT
      AS STRUCT arr[
    OFFSET
      (0)] KEY, arr[
    OFFSET
      (1)] value
    FROM
      UNNEST(REGEXP_EXTRACT_ALL(option_value, r'STRUCT\(("[^"]+", "[^"]+")\)')) kv, UNNEST([STRUCT(SPLIT(REPLACE(kv, '"', ''), ', ') AS arr)]) ) AS labels
  FROM
    `isg-dwh-bigquery`.`region-EU`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
  WHERE
    option_name IN ('labels') ),
  UNNEST(labels)
WHERE
  KEY='ga_analytics_view'
ORDER BY
  2,
  3;
