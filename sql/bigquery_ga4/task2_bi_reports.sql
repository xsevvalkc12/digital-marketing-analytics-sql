CREATE OR REPLACE TABLE `myproject1-476715.ga4_analysis.events_2021_funnel` AS
SELECT
  TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
  user_pseudo_id,

  (SELECT ep.value.int_value
   FROM UNNEST(event_params) ep
   WHERE ep.key = 'ga_session_id') AS session_id,

  event_name,
  geo.country AS country,
  device.category AS device_category,

  COALESCE(
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'source'),
    traffic_source.source
  ) AS source,

  COALESCE(
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'medium'),
    traffic_source.medium
  ) AS medium,

  COALESCE(
    (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'campaign'),
    traffic_source.name
  ) AS campaign

FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'
  AND event_name IN (
    'session_start',
    'view_item',
    'add_to_cart',
    'begin_checkout',
    'add_shipping_info',
    'add_payment_info',
    'purchase'
  );


SELECT
  event_name,
  COUNT(*) AS cnt
FROM `myproject1-476715.ga4_analysis.events_2021_funnel`
GROUP BY event_name
ORDER BY cnt DESC;

