--To Get actual data you need to set a timezone as well
--otherwise it will be UTC
  
WITH
  date_range AS (
  SELECT
    FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 day)) AS start_date),
  data_check AS (
  SELECT
    EXISTS(
    SELECT
      1
    FROM
      `bigquery.analytics_123456789.events_*`,
      date_range
    WHERE
      _table_suffix BETWEEN start_date
      AND start_date) AS condition)
SELECT
  PARSE_DATE('%Y%m%d',event_date) AS date_event,
  * EXCEPT(start_date,
    condition)
FROM
  `bigquery.analytics_123456789.events_*`,
  date_range,
  data_check
WHERE
  condition=TRUE
  AND _table_suffix BETWEEN start_date
  AND start_date
  AND EXTRACT(hour
  FROM
    TIMESTAMP_MICROS(event_timestamp)) BETWEEN 19
  AND 20
