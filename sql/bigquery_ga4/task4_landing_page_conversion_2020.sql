CREATE OR REPLACE TABLE `myproject1-476715.ga4_analysis.landing_page_conversion_2020` AS
WITH base AS (
  SELECT
    user_pseudo_id,
    (SELECT ep.value.int_value
     FROM UNNEST(event_params) ep
     WHERE ep.key = 'ga_session_id') AS session_id,

    event_name,

    REGEXP_EXTRACT(
      (SELECT ep.value.string_value
       FROM UNNEST(event_params) ep
       WHERE ep.key = 'page_location'),
      r'https?://[^/]+(/[^?#]*)'
    ) AS page_path
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'
    AND event_name IN ('session_start', 'purchase')
),

session_landing AS (
  SELECT
    user_pseudo_id,
    session_id,

    MAX(IF(event_name = 'session_start', page_path, NULL)) AS landing_page,

    MAX(IF(event_name = 'purchase', 1, 0)) AS has_purchase
  FROM base
  WHERE session_id IS NOT NULL
  GROUP BY 1,2
)

SELECT
  landing_page,
  COUNT(*) AS unique_sessions,
  SUM(has_purchase) AS purchases,
  SAFE_DIVIDE(SUM(has_purchase), COUNT(*)) AS purchase_conversion_rate
FROM session_landing
WHERE landing_page IS NOT NULL
GROUP BY landing_page
ORDER BY purchase_conversion_rate DESC;

SELECT *
FROM `myproject1-476715.ga4_analysis.landing_page_conversion_2020`
ORDER BY purchase_conversion_rate DESC
LIMIT 20;

