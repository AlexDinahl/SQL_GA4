SELECT *, DATETIME(TIMESTAMP_MILLIS(last_modified_time),'Europe/Berlin') as last_modified
FROM `analytics_292798251.__TABLES__`
where table_id LIKE 'events_2%'
