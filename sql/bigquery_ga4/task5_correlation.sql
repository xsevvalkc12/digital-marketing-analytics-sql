CREATE OR REPLACE TABLE `myproject1-476715.ga4_analysis.session_engagement_purchase_2021` AS
WITH base AS (
  SELECT
    user_pseudo_id,

    (SELECT ep.value.int_value
     FROM UNNEST(event_params) ep
     WHERE ep.key = 'ga_session_id') AS session_id,

    MAX(
      IF(
        (SELECT ep.value.string_value
         FROM UNNEST(event_params) ep
         WHERE ep.key = 'session_engaged') = '1',
        1, 0
      )
    ) AS session_engaged,

    SUM(
      COALESCE(
        (SELECT ep.value.int_value
         FROM UNNEST(event_params) ep
         WHERE ep.key = 'engagement_time_msec'),
        0
      )
    ) AS engagement_time_msec,

    MAX(
      IF(event_name = 'purchase', 1, 0)
    ) AS has_purchase

  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'
  GROUP BY user_pseudo_id, session_id
)

SELECT
  user_pseudo_id,
  session_id,
  session_engaged,
  engagement_time_msec,
  has_purchase
FROM base
WHERE session_id IS NOT NULL;

SELECT
  CORR(session_engaged, has_purchase) AS corr_engaged_vs_purchase,
  CORR(engagement_time_msec, has_purchase) AS corr_time_vs_purchase
FROM `myproject1-476715.ga4_analysis.session_engagement_purchase_2021`;





