# Exploratory Data Analysis (EDA)

This directory contains exploratory analysis scripts and figures for HAHD gaze and driving behavior data.

## Scripts

- **`screen_dimensions_viewport_analysis.py`** — Analyzes participant screen resolution distribution to inform gaze normalization decisions
- **`video_duration_analysis_and_truncation_recommendations.py`** — Examines raw video durations and recommends truncation thresholds for consistent clip lengths
- **`gaze_retention_analysis_and_filtering_thresholds.py`** — Evaluates what fraction of gaze samples survive various quality filters; used to set the bad-gaze cutoffs in the ETL transform step
- **`EDAf.ipynb`** — Jupyter notebook with full exploratory analysis including gaze heatmaps, per-user quality checks, and feature distribution plots

## Outputs

- **`eda_figures/`** — Generated plots and visualizations
- **`eda_data/`** — Intermediate analysis outputs

## Running

```bash
# Run individual analysis scripts
python eda/screen_dimensions_viewport_analysis.py
python eda/video_duration_analysis_and_truncation_recommendations.py
python eda/gaze_retention_analysis_and_filtering_thresholds.py
```

Or open the notebook:

```bash
jupyter notebook eda/EDAf.ipynb
```

## Requirements
- Python 3.x
- `pandas`, `matplotlib`, `seaborn`, `jupyter`
- Processed data in `data/processed/` (run ETL pipeline first)
