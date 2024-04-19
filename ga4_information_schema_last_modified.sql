--Get table info and date modified

SELECT
  *,
  DATETIME(TIMESTAMP_MILLIS(last_modified_time),'Europe/Berlin') AS last_modified
FROM
  `analytics_123456789.__TABLES__`
WHERE
  table_id LIKE 'events_2%'
