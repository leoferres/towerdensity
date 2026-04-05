-- 02_cell_od.sql
-- Anonymized for replication package. Dataset and table names have been
-- replaced with generic identifiers; adapt to your own BigQuery project.
--
-- Build a cell-to-cell origin-destination matrix from consecutive XDR events.

DECLARE DATE_FROM     DATE    DEFAULT '2023-08-01';
DECLARE DATE_TO       DATE    DEFAULT '2023-09-14';
DECLARE MAX_GAP_HOURS INT64   DEFAULT 6;

CREATE OR REPLACE TABLE mobility_data.td_cell_od AS

WITH events AS (
  SELECT
    x.PHONE_ID,
    x.CELL_ID,
    c.bts_id,
    c.codcom,
    c.lat,
    c.lon,
    x.TIMESTAMP
  FROM `mobility_data.xdr_events` x
  JOIN `mobility_data.cell_catalog` c
    ON x.CELL_ID = c.CELL_ID
  JOIN `mobility_data.td_valid_users_r13` v
    ON x.PHONE_ID = v.PHONE_ID
  WHERE c.codregion = 13
    AND x.TIMESTAMP >= TIMESTAMP(DATE_FROM)
    AND x.TIMESTAMP <  TIMESTAMP(DATE_ADD(DATE_TO, INTERVAL 1 DAY))
),

transitions AS (
  SELECT
    PHONE_ID,
    CELL_ID                                                                   AS cell_from,
    LEAD(CELL_ID)    OVER (PARTITION BY PHONE_ID ORDER BY TIMESTAMP)          AS cell_to,
    bts_id                                                                    AS bts_from,
    LEAD(bts_id)     OVER (PARTITION BY PHONE_ID ORDER BY TIMESTAMP)          AS bts_to,
    codcom                                                                    AS codcom_from,
    LEAD(codcom)     OVER (PARTITION BY PHONE_ID ORDER BY TIMESTAMP)          AS codcom_to,
    lat                                                                       AS lat_from,
    lon                                                                       AS lon_from,
    LEAD(lat)        OVER (PARTITION BY PHONE_ID ORDER BY TIMESTAMP)          AS lat_to,
    LEAD(lon)        OVER (PARTITION BY PHONE_ID ORDER BY TIMESTAMP)          AS lon_to,
    TIMESTAMP                                                                 AS ts_from,
    LEAD(TIMESTAMP)  OVER (PARTITION BY PHONE_ID ORDER BY TIMESTAMP)          AS ts_to
  FROM events
)

SELECT
  cell_from,
  cell_to,
  bts_from,
  bts_to,
  codcom_from,
  codcom_to,
  ANY_VALUE(lat_from)                                                   AS lat_from,
  ANY_VALUE(lon_from)                                                   AS lon_from,
  ANY_VALUE(lat_to)                                                     AS lat_to,
  ANY_VALUE(lon_to)                                                     AS lon_to,
  (bts_from = bts_to AND cell_from != cell_to)                          AS is_intrasite,
  COUNT(DISTINCT PHONE_ID)                                              AS n_users,
  COUNT(*)                                                              AS n_trips,
  AVG(TIMESTAMP_DIFF(ts_to, ts_from, MINUTE))                          AS avg_duration_min,
  APPROX_QUANTILES(TIMESTAMP_DIFF(ts_to, ts_from, MINUTE), 100)[OFFSET(50)] AS median_duration_min
FROM transitions
WHERE cell_to IS NOT NULL
  AND cell_from != cell_to
  AND TIMESTAMP_DIFF(ts_to, ts_from, HOUR) <= MAX_GAP_HOURS
GROUP BY
  cell_from, cell_to, bts_from, bts_to,
  codcom_from, codcom_to,
  is_intrasite
