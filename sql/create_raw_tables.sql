-- 목적:
--   1) raw/mart 스키마를 만들고
--   2) CSV 로딩 안정성을 위해 raw.event_date 를 VARCHAR로 고정합니다.
--      (CSV → DuckDB 로딩 시 DATE 타입 추론/파싱 이슈를 피함)
--   3) Mart 단계에서 CAST(event_date AS DATE)로 정규화합니다.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;

-- 광고 이벤트(노출/클릭/전환)
CREATE TABLE IF NOT EXISTS raw.ad_events (
  event_date  VARCHAR,        -- 'YYYY-MM-DD' 문자열로 저장 (로딩 안정성)
  event_ts    TIMESTAMP,      -- 이벤트 발생 시각(ISO timestamp로 CSV에 저장)
  event_type  VARCHAR,        -- impression / click / conversion
  campaign_id VARCHAR,
  ad_id       VARCHAR,
  user_id     VARCHAR,
  device_os   VARCHAR,        -- iOS / Android / Web
  country     VARCHAR,
  cost        DOUBLE,         -- 클릭 비용 등
  revenue     DOUBLE          -- 전환 매출(전환 이벤트에서만 >0)
);

-- 결제 이벤트(성공/실패)
CREATE TABLE IF NOT EXISTS raw.payment_events (
  event_date   VARCHAR,       -- 'YYYY-MM-DD'
  event_ts     TIMESTAMP,
  order_id     VARCHAR,
  user_id      VARCHAR,
  campaign_id  VARCHAR,
  amount       DOUBLE,
  currency     VARCHAR,       -- KRW 등
  status       VARCHAR,       -- success / failed
  fail_reason  VARCHAR        -- status=failed일 때만 값 존재
);