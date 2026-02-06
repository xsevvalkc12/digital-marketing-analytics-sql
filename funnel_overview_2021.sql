CREATE OR REPLACE TABLE `myproject1-476715.ga4_analysis.funnel_overview_2021` AS
SELECT 'Sessions' AS stage, SUM(user_sessions_count) AS value
FROM `myproject1-476715.ga4_analysis.conversion_by_date_channel_2021`

UNION ALL
SELECT 'Add to Cart' AS stage, SUM(visit_to_cart) AS value
FROM `myproject1-476715.ga4_analysis.conversion_by_date_channel_2021`

UNION ALL
SELECT 'Checkout' AS stage, SUM(visit_to_checkout) AS value
FROM `myproject1-476715.ga4_analysis.conversion_by_date_channel_2021`

UNION ALL
SELECT 'Purchase' AS stage, SUM(visit_to_purchase) AS value
FROM `myproject1-476715.ga4_analysis.conversion_by_date_channel_2021`;
