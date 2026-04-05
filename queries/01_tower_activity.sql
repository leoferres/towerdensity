-- 01_tower_activity.sql
-- Anonymized for replication package. Dataset and table names have been
-- replaced with generic identifiers; adapt to your own BigQuery project.
--
-- Compute per-cell activity counts for valid R13 users.

DECLARE DATE_FROM DATE DEFAULT '2023-08-01';
DECLARE DATE_TO   DATE DEFAULT '2023-09-14';

CREATE OR REPLACE TABLE mobility_data.td_tower_activity AS

WITH events AS (
  SELECT
    x.PHONE_ID,
    x.CELL_ID,
    c.bts_id,
    c.codcom,
    c.codregion,
    c.lat,
    c.lon,
    x.TIMESTAMP,
    EXTRACT(HOUR FROM x.TIMESTAMP) AS local_hour
  FROM `mobility_data.xdr_events` x
  JOIN `mobility_data.cell_catalog` c
    ON x.CELL_ID = c.CELL_ID
  JOIN `mobility_data.td_valid_users_r13` v
    ON x.PHONE_ID = v.PHONE_ID
  WHERE c.codregion = 13
    AND x.TIMESTAMP >= TIMESTAMP(DATE_FROM)
    AND x.TIMESTAMP <  TIMESTAMP(DATE_ADD(DATE_TO, INTERVAL 1 DAY))
)

SELECT
  CELL_ID,
  bts_id,
  codcom,
  codregion,
  lat,
  lon,
  COUNT(DISTINCT PHONE_ID)                                              AS n_users_total,
  COUNT(*)                                                              AS n_events_total,
  COUNT(DISTINCT IF(local_hour BETWEEN 6 AND 21, PHONE_ID, NULL))      AS n_users_daytime,
  COUNT(DISTINCT IF(local_hour < 6 OR local_hour >= 22, PHONE_ID, NULL)) AS n_users_nighttime,
  COUNTIF(local_hour BETWEEN 6 AND 21)                                 AS n_events_daytime,
  COUNTIF(local_hour < 6 OR local_hour >= 22)                          AS n_events_nighttime
FROM events
GROUP BY CELL_ID, bts_id, codcom, codregion, lat, lon
