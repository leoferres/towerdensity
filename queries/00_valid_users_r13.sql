-- 00_valid_users_r13.sql
-- Anonymized for replication package. Dataset and table names have been
-- replaced with generic identifiers; adapt to your own BigQuery project.
--
-- Filter users active in Region Metropolitana (R13) with at least 2 distinct BTS.
-- Removes stationary devices (IoT, modems, alarm systems) and out-of-region users.
--
-- Input:  mobility_data.xdr_events, mobility_data.cell_catalog
-- Output: mobility_data.td_valid_users_r13
-- Fields: PHONE_ID, n_bts_r13, n_events_r13, n_active_days_r13

DECLARE DATE_FROM DATE DEFAULT '2023-08-01';
DECLARE DATE_TO   DATE DEFAULT '2023-09-14';

CREATE OR REPLACE TABLE mobility_data.td_valid_users_r13 AS

WITH r13_events AS (
  SELECT
    x.PHONE_ID,
    c.bts_id,
    DATE(x.TIMESTAMP) AS event_date
  FROM `mobility_data.xdr_events` x
  JOIN `mobility_data.cell_catalog` c
    ON x.CELL_ID = c.CELL_ID
  WHERE c.codregion = 13
    AND x.TIMESTAMP >= TIMESTAMP(DATE_FROM)
    AND x.TIMESTAMP <  TIMESTAMP(DATE_ADD(DATE_TO, INTERVAL 1 DAY))
),

user_summary AS (
  SELECT
    PHONE_ID,
    COUNT(DISTINCT bts_id)   AS n_bts_r13,
    COUNT(*)                 AS n_events_r13,
    COUNT(DISTINCT event_date) AS n_active_days_r13
  FROM r13_events
  GROUP BY PHONE_ID
)

SELECT *
FROM user_summary
WHERE n_bts_r13 >= 2
