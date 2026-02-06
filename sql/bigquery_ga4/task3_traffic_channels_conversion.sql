CREATE OR REPLACE TABLE `myproject1-476715.ga4_analysis.conversion_by_date_channel_2021` AS
WITH base AS (
  SELECT
    DATE(event_timestamp) AS event_date,
    user_pseudo_id,
    CAST(session_id AS INT64) AS session_id,
    event_name,
    source,
    medium,
    campaign
  FROM `myproject1-476715.ga4_analysis.events_2021_funnel`
  WHERE session_id IS NOT NULL
),

session_level AS (
  SELECT
    event_date,
    source,
    medium,
    campaign,
    user_pseudo_id,
    session_id,

    MAX(IF(event_name = 'session_start', 1, 0)) AS has_session_start,

    MAX(IF(event_name = 'add_to_cart', 1, 0)) AS has_add_to_cart,
    MAX(IF(event_name IN ('begin_checkout','add_shipping_info','add_payment_info'), 1, 0)) AS has_checkout,
    MAX(IF(event_name = 'purchase', 1, 0)) AS has_purchase
  FROM base
  GROUP BY 1,2,3,4,5,6
)

SELECT
  event_date,
  source,
  medium,
  campaign,

  COUNTIF(has_session_start = 1) AS user_sessions_count,

  SAFE_DIVIDE(COUNTIF(has_session_start = 1 AND has_add_to_cart = 1), COUNTIF(has_session_start = 1)) AS visit_to_cart,
  SAFE_DIVIDE(COUNTIF(has_session_start = 1 AND has_checkout = 1),    COUNTIF(has_session_start = 1)) AS visit_to_checkout,
  SAFE_DIVIDE(COUNTIF(has_session_start = 1 AND has_purchase = 1),    COUNTIF(has_session_start = 1)) AS visit_to_purchase

FROM session_level
GROUP BY 1,2,3,4
ORDER BY event_date, source, medium, campaign;


SELECT *
FROM `myproject1-476715.ga4_analysis.conversion_by_date_channel_2021`
ORDER BY event_date DESC
LIMIT 50;
