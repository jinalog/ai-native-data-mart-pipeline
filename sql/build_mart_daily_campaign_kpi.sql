-- 목적:
--   raw 레이어(문자열 event_date)를 mart 레이어(날짜형 event_date)로 정규화하여
--   일별/캠페인별 KPI를 생성합니다.

DROP TABLE IF EXISTS mart.daily_campaign_kpi;

CREATE TABLE mart.daily_campaign_kpi AS
WITH ad AS (
  SELECT
    CAST(event_date AS DATE) AS event_date,         -- 여기서 DATE로 정규화
    campaign_id,

    SUM(CASE WHEN event_type='impression' THEN 1 ELSE 0 END) AS impressions,
    SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END)      AS clicks,
    SUM(CASE WHEN event_type='conversion' THEN 1 ELSE 0 END) AS conversions,

    SUM(cost)    AS ad_cost,
    SUM(revenue) AS ad_revenue
  FROM raw.ad_events
  GROUP BY 1,2
),
pay AS (
  SELECT
    CAST(event_date AS DATE) AS event_date,
    campaign_id,

    COUNT(*) AS payments_total,
    SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS payments_success,
    SUM(CASE WHEN status='failed'  THEN 1 ELSE 0 END) AS payments_failed,

    SUM(CASE WHEN status='success' THEN amount ELSE 0 END) AS pay_amount_success
  FROM raw.payment_events
  GROUP BY 1,2
)
SELECT
  COALESCE(ad.event_date, pay.event_date)       AS event_date,
  COALESCE(ad.campaign_id, pay.campaign_id)     AS campaign_id,

  COALESCE(impressions, 0) AS impressions,
  COALESCE(clicks, 0)      AS clicks,
  COALESCE(conversions, 0) AS conversions,

  CASE
    WHEN COALESCE(impressions,0)=0 THEN 0
    ELSE COALESCE(clicks,0)::DOUBLE / impressions
  END AS ctr,

  CASE
    WHEN COALESCE(clicks,0)=0 THEN 0
    ELSE COALESCE(conversions,0)::DOUBLE / clicks
  END AS cvr,

  COALESCE(ad_cost, 0)    AS ad_cost,
  COALESCE(ad_revenue, 0) AS ad_revenue,

  COALESCE(payments_total, 0)   AS payments_total,
  COALESCE(payments_success, 0) AS payments_success,
  COALESCE(payments_failed, 0)  AS payments_failed,

  CASE
    WHEN COALESCE(payments_total,0)=0 THEN 0
    ELSE COALESCE(payments_success,0)::DOUBLE / payments_total
  END AS payment_success_rate,

  COALESCE(pay_amount_success, 0) AS pay_amount_success

FROM ad
FULL OUTER JOIN pay
  ON ad.event_date = pay.event_date
 AND ad.campaign_id = pay.campaign_id;