-- 04_user_stats.sql
-- Anonymized for replication package. Dataset and table names have been
-- replaced with generic identifiers; adapt to your own BigQuery project.
--
-- Per-user statistics for inverse probability weighting (IPW) in nb05.

DECLARE DATE_FROM   DATE  DEFAULT '2023-08-01';
DECLARE DATE_TO     DATE  DEFAULT '2023-09-14';

CREATE OR REPLACE TABLE mobility_data.td_user_stats AS

WITH events AS (
  SELECT
    x.PHONE_ID,
    x.CELL_ID,
    c.bts_id,
    c.codcom,
    c.lat,
    c.lon,
    x.TIMESTAMP,
    DATE(x.TIMESTAMP)              AS event_date,
    EXTRACT(HOUR FROM x.TIMESTAMP) AS local_hour
  FROM `mobility_data.xdr_events` x
  JOIN `mobility_data.cell_catalog` c
    ON x.CELL_ID = c.CELL_ID
  JOIN `mobility_data.td_valid_users_r13` v
    ON x.PHONE_ID = v.PHONE_ID
  WHERE c.codregion = 13
    AND x.TIMESTAMP >= TIMESTAMP(DATE_FROM)
    AND x.TIMESTAMP <  TIMESTAMP(DATE_ADD(DATE_TO, INTERVAL 1 DAY))
),

nighttime_bts AS (
  SELECT
    PHONE_ID,
    bts_id,
    codcom,
    lat,
    lon,
    COUNT(*) AS n_nighttime_events
  FROM events
  WHERE local_hour < 6 OR local_hour >= 22
  GROUP BY PHONE_ID, bts_id, codcom, lat, lon
),

home_bts AS (
  SELECT
    PHONE_ID,
    bts_id           AS home_bts,
    codcom           AS home_codcom,
    lat              AS home_lat,
    lon              AS home_lon,
    n_nighttime_events
  FROM nighttime_bts
  QUALIFY ROW_NUMBER() OVER (PARTITION BY PHONE_ID ORDER BY n_nighttime_events DESC) = 1
),

user_summary AS (
  SELECT
    PHONE_ID,
    COUNT(DISTINCT bts_id)      AS n_bts_r13,
    COUNT(*)                    AS n_events_r13,
    COUNT(DISTINCT event_date)  AS n_active_days,
    DATE_DIFF(MAX(event_date), MIN(event_date), DAY) + 1 AS obs_days_span
  FROM events
  GROUP BY PHONE_ID
)

SELECT
  u.PHONE_ID,
  u.n_bts_r13,
  u.n_events_r13,
  u.n_active_days,
  u.obs_days_span,
  h.home_bts,
  h.home_codcom,
  h.home_lat,
  h.home_lon
FROM user_summary u
LEFT JOIN home_bts h USING (PHONE_ID)
