# Replication Package: Systematic Biases in Mobile Phone Mobility Data from Heterogeneous Tower Density

**Authors:** Leo Ferres and Erick Elejalde

This package replicates the figures and key results presented in the paper. The pipeline is organized as a sequence of six Jupyter notebooks that read raw inputs, produce intermediate datasets, and generate all outputs.

## Data Requirements

| Dataset | Description | Availability |
|---------|-------------|--------------|
| Tower catalog CSV | Columns: `cell_id`, `bts_id`, `tech`, `admin_code`, `lat`, `lon`, `height`, `azimuth` | Proprietary -- not included |
| Census cartography | 2024 Chilean census for Region 13 (Santiago) | Publicly available from INE (www.ine.gob.cl) |
| XDR-derived BigQuery exports | `td_cell_od`, `td_bts_od`, `td_tower_activity` | Proprietary -- not included. The SQL queries used to generate these tables from raw XDR data are provided in `queries/` |
| OSM road network | OpenStreetMap road geometries for Santiago | Downloaded automatically by the code via `osmnx` |

## Repository Structure

```
replication/
  README.md
  environment.yml
  notebooks/
    01_tower_geometry.ipynb
    02_detection_floor.ipynb
    03_census_redistribution.ipynb
    04_od_matrix.ipynb
    05_ipw_correction.ipynb
    06_fay_herriot.ipynb
  queries/
    00_valid_users_r13.sql
    01_tower_activity.sql
    02_cell_od.sql
    03_bts_od.sql
    04_user_stats.sql
  data/          (created at runtime by the notebooks)
```

## Instructions

1. Create the conda environment and activate it:

   ```bash
   conda env create -f environment.yml
   conda activate mobilens
   ```

2. Run the six notebooks in order (01 through 06). Each notebook reads its inputs from `data/` and saves intermediate parquet files back to `data/` for downstream notebooks.

   ```
   nb01 -> nb02 -> nb03 -> nb04 -> nb05 -> nb06
   ```

3. The SQL queries in `queries/` document how the proprietary BigQuery tables (`td_cell_od`, `td_bts_od`, `td_tower_activity`) were derived from raw XDR records. They are provided for transparency and are not executed by the notebooks directly.

## mobilens Library

A reusable Python implementation of the full pipeline is available as the `mobilens` library:

  https://github.com/leoferres/mobilens

## Environment

The environment is specified in `environment.yml` and can be installed with conda (or mamba). Python 3.11 is required. Key dependencies include geopandas, h3-py, osmnx, statsmodels, and the standard scientific Python stack.
