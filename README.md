# End-to-End Data WorkFlow

This repository includes data, ETL scripts, utilities, and notebooks used to load, clean, transform, and analyze sample orders and users data.

**Contents**

- `data/` — raw and processed CSV data used by the exercises and ETL pipeline.
- `src/bootcamp_data/` — core modules for loading, transforming, joining, and validating data.
- `scripts/` — runner scripts to execute the ETL steps (`run_day1_load.py`, `run_day2_load.py`, `run_day3_load.py`, `run_ETL.py`).
- `notebooks/` — EDA notebook.
- `reports/` — generated CSV reports and figures from the pipeline and summary.md

**Quick summary**

The project demonstrates a small ETL workflow: loading raw CSVs from `data/raw/`, applying cleaning and transformation logic in `src/bootcamp_data/`, and writing processed outputs to `data/processed/` and `reports/`.

**How to set up**

1. Create and activate a Python environment (recommended):

	 python3 -m venv .venv
	 source .venv/bin/activate

2. Install dependencies:

	 pip install -r requirements.txt

**How to run the ETL**

- Run the full ETL (recommended from the `scripts/` folder):

	python run_ETL.py

These scripts call functions in `src/bootcamp_data/` to load, clean, and persist processed files.


**Notebooks & analysis**

See `notebooks/EDA.ipynb` for exploratory analysis and examples of how to inspect processed data and generate figures.

**Important note:** The data used by `notebooks/EDA.ipynb` differs from the outputs created by running `scripts/run_ETL.py`. The notebook was prepared using an expanded dataset (to provide more examples), while `run_ETL.py` generates the cleaned/standardized processed outputs in `data/processed/`.

**Next steps**

- Run `python scripts/run_ETL.py` to regenerate processed data and reports.
- Add unit tests for key transform functions in `src/bootcamp_data/`.
- Version the processed outputs or add sample snapshots under `data/processed/` for reproducibility.

## Outputs

- `data/processed/analytics_table.parquet` — processed analytics table used by downstream analysis.
- `data/processed/_run_meta.json` — pipeline run metadata and provenance.
- `reports/figures/*.png` — generated figures from notebooks or reporting scripts.