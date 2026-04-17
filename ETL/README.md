# ETL Pipeline

This directory contains the extract, transform, and process pipeline for HAHD gaze and survey data.

## Overview

The ETL pipeline pulls raw data from MongoDB and AWS S3, transforms gaze coordinates and survey responses into analysis-ready formats, and outputs processed CSVs and video files to `data/processed/`.

## Pipeline Stages

### 1. Extract — `extractData.py`
Pulls raw data from two sources:
- **MongoDB**: Survey responses and user metadata
- **AWS S3**: Raw driving video footage

Outputs raw files to `data/raw/`.

**Requires**: `MONGO_URI`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET_NAME` set in `.env`.

### 2. Transform — `transformData.py`
Normalizes raw gaze coordinates against each participant's recorded screen resolution, filters bad gaze samples, and bins gaze data by video timestamp.

Outputs:
- `data/processed/normalized_gaze_data.csv`
- `data/processed/badgazedata.csv`
- `data/processed/binned_video_dat_wo_user.csv`
- `data/processed/aggregate_gaze_data_by_video.csv`

### 3. Process — `processData.py`
Merges transformed gaze data with survey responses and user metadata into a single analysis-ready dataset.

Outputs:
- `data/processed/final_user_survey_data.csv`

## Running the Full Pipeline

```bash
python main.py
```

Or run each stage individually:

```bash
python -c "from etl.extractData import GazeDataExtractor; GazeDataExtractor('./data').extract_data()"
python -c "from etl.transformData import GazeDataTransformer; GazeDataTransformer(data_dir='./data').transform_data()"
python -c "from etl.processData import DataProcessor; DataProcessor(data_dir='./data').process_data()"
```

## Requirements
- Python 3.x
- `boto3` for S3 access
- `pymongo` for MongoDB access
- See `requirements.txt` for full list
