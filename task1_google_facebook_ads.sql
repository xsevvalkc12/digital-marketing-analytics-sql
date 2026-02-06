--1)Google ve Facebook için günlük spend toplamlarının avg / max / min değerleri (ayrı ayrı)
WITH fb_daily AS (
  SELECT
    ad_date,
    SUM(spend)::numeric AS daily_spend
  FROM public.facebook_ads_basic_daily
  GROUP BY ad_date
),
gg_daily AS (
  SELECT
    ad_date,
    SUM(spend)::numeric AS daily_spend
  FROM public.google_ads_basic_daily
  GROUP BY ad_date
)
SELECT
  'facebook' AS source,
  ROUND(AVG(daily_spend), 2) AS avg_daily_spend,
  ROUND(MAX(daily_spend), 2) AS max_daily_spend,
  ROUND(MIN(daily_spend), 2) AS min_daily_spend
FROM fb_daily
UNION ALL
SELECT
  'google' AS source,
  ROUND(AVG(daily_spend), 2) AS avg_daily_spend,
  ROUND(MAX(daily_spend), 2) AS max_daily_spend,
  ROUND(MIN(daily_spend), 2) AS min_daily_spend
FROM gg_daily;


--2) Toplam ROMI (Google+Facebook) açısından en yüksek 5 gün (tarih + romi)
WITH all_daily AS (
  SELECT
    ad_date,
    SUM(COALESCE(spend, 0))::numeric AS spend,
    SUM(COALESCE(value, 0))::numeric AS value
  FROM (
    SELECT ad_date, spend, value
    FROM public.facebook_ads_basic_daily
    UNION ALL
    SELECT ad_date, spend, value
    FROM public.google_ads_basic_daily
  ) t
  GROUP BY ad_date
)
SELECT
  ad_date,
  ROUND((value - spend) / spend, 4) AS romi
FROM all_daily
WHERE spend > 0
ORDER BY romi DESC
LIMIT 5;

--3) Haftalık en yüksek toplam value’ya sahip kampanya (hafta + rekor değer)
WITH unioned AS (
SELECT
    f.ad_date,
    fc.campaign_name AS campaign_name,
    COALESCE(f.value, 0)::numeric AS value
  FROM public.facebook_ads_basic_daily f
  LEFT JOIN public.facebook_campaign fc
    ON fc.campaign_id = f.campaign_id

  UNION all
  
  SELECT
    g.ad_date,
    g.campaign_name AS campaign_name,
    COALESCE(g.value, 0)::numeric AS value
  FROM public.google_ads_basic_daily g
),
weekly AS (
  SELECT
    date_trunc('week', ad_date)::date AS week_start,
    campaign_name,
    SUM(value) AS weekly_value
  FROM unioned
  GROUP BY 1, 2
)
SELECT
  week_start,
  campaign_name,
  ROUND(weekly_value, 2) AS weekly_value
FROM weekly
ORDER BY weekly_value DESC
LIMIT 1;

--4) Aylık bazda en büyük reach artışı yaşayan kampanya (MoM artış)
WITH unioned AS (
  SELECT
    f.ad_date,
    fc.campaign_name AS campaign_name,
    f.reach::numeric AS reach
  FROM public.facebook_ads_basic_daily f
  LEFT JOIN public.facebook_campaign fc
    ON fc.campaign_id = f.campaign_id

  UNION ALL

  SELECT
    g.ad_date,
    g.campaign_name  AS campaign_name,
    g.reach::numeric AS reach
  FROM public.google_ads_basic_daily g
),
monthly AS (
  SELECT
    date_trunc('month', ad_date)::date AS month_start,
    campaign_name,
    SUM(reach) AS monthly_reach
  FROM unioned
  GROUP BY 1, 2
),
diffs AS (
  SELECT
    month_start,
    campaign_name,
    monthly_reach,
    LAG(monthly_reach) OVER (PARTITION BY campaign_name ORDER BY month_start) AS prev_month_reach
  FROM monthly
)
SELECT
  month_start,
  campaign_name,
  ROUND(monthly_reach - prev_month_reach, 2) AS reach_increase
FROM diffs
WHERE prev_month_reach IS NOT NULL
ORDER BY reach_increase DESC
LIMIT 1;

--5) (Google + Facebook) en uzun kesintisiz impressions’a sahip adset_name ve süresi
WITH unioned AS (
  -- Facebook: adset_id -> adset_nar
  SELECT
    f.ad_date,
    fa.adset_name AS adset_name,
    SUM(f.impressions)::bigint AS impressions
  FROM public.facebook_ads_basic_daily f
  LEFT JOIN public.facebook_adset fa
    ON fa.adset_id = f.adset_id
  GROUP BY f.ad_date, fa.adset_name

  UNION ALL

  -- Google: adset_name direkt var
  SELECT
    g.ad_date,
    g.adset_name AS adset_name,
    SUM(g.impressions)::bigint AS impressions
  FROM public.google_ads_basic_daily g
  GROUP BY g.ad_date, g.adset_name
),
positive_days AS (
  SELECT
    ad_date,
    adset_name
  FROM unioned
  WHERE COALESCE(impressions, 0) > 0
    AND adset_name IS NOT NULL
),
streaks AS (
  SELECT
    adset_name,
    ad_date,
    (ad_date - (ROW_NUMBER() OVER (PARTITION BY adset_name ORDER BY ad_date))::int) AS grp_key
  FROM positive_days
),
streak_agg AS (
  SELECT
    adset_name,
    MIN(ad_date) AS start_date,
    MAX(ad_date) AS end_date,
    (MAX(ad_date) - MIN(ad_date) + 1) AS duration_days
  FROM streaks
  GROUP BY adset_name, grp_key
)
SELECT
  adset_name,
  start_date,
  end_date,
  duration_days
FROM streak_agg
ORDER BY duration_days DESC
LIMIT 1;

