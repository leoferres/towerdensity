-- 03_bts_od.sql
-- Anonymized for replication package. Dataset and table names have been
-- replaced with generic identifiers; adapt to your own BigQuery project.
--
-- Aggregate cell-level OD matrix to BTS level.

CREATE OR REPLACE TABLE mobility_data.td_bts_od AS

WITH bts_codcom AS (
  SELECT
    bts_id,
    codcom,
    COUNT(*) AS cnt
  FROM `mobility_data.cell_catalog`
  GROUP BY bts_id, codcom
  QUALIFY ROW_NUMBER() OVER (PARTITION BY bts_id ORDER BY cnt DESC) = 1
),

aggregated AS (
  SELECT
    bts_from,
    bts_to,
    is_intrasite,
    COUNT(DISTINCT cell_from)  AS n_cells_from,
    COUNT(DISTINCT cell_to)    AS n_cells_to,
    SUM(n_users)               AS n_users,
    SUM(n_trips)               AS n_trips,
    SUM(n_trips * avg_duration_min) / NULLIF(SUM(n_trips), 0) AS avg_duration_min
  FROM `mobility_data.td_cell_od`
  GROUP BY bts_from, bts_to, is_intrasite
)

SELECT
  a.bts_from,
  a.bts_to,
  bf.codcom  AS codcom_from,
  bt.codcom  AS codcom_to,
  a.is_intrasite,
  a.n_cells_from,
  a.n_cells_to,
  a.n_users,
  a.n_trips,
  a.avg_duration_min
FROM aggregated a
LEFT JOIN bts_codcom bf ON a.bts_from = bf.bts_id
LEFT JOIN bts_codcom bt ON a.bts_to   = bt.bts_id
