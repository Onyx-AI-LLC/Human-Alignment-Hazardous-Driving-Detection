# Human-Aligned Hazardous Driving (HAHD) Project

## Overview
The Human-Aligned Hazardous Driving (HAHD) project is an initiative focused on collecting, processing, and analyzing driving behavior data to train machine learning models that align autonomous vehicle decision-making with human driving tendencies. This project consists of three main components:

---
## Folder Structure 
```
HAHD/
├── data/
│   ├── processed/              # Processed data after running the transform and processing (etl)
│   |       ├── driving_videos/ 
│   |       ├── badgazedata.csv 
│   |       ├── normalized_gaze_data.csv 
│   |       ├── final_user_survey_data.csv
│   |       ├── binned_video_dat_wo_user.csv
│   |       ├── aggregate_gaze_data_by_video.csv
│   ├── raw/
│   |       ├── driving_videos/ # Videos from S3 after running extraction (etl)
│   |       ├── survey_results_raw.csv # Data from MongoDB running extraction (etl)
│   |       ├── users_data.csv # Data from MongoDB running extraction (etl)
├── eda/                        # Exploratory data analysis scripts and figures
├── etl/                        # ETL pipeline: extract, transform, process
├── expeirments/                # Model experiments and evaluation notebooks
├── frontend/                   # React/TypeScript frontend for the data collection web app
├── models/                     # Model checkpoints (generated at runtime)
├── research/                   # Research paper and supporting materials
├── server/                     # Node.js backend for the data collection web app
├── video-ingestion/            # Scripts to process raw Tesla footage before S3 upload
├── README.md  
├── package.json 
├── package-lock.json                  
├── .gitignore    
├── requirements.txt    
├── main.py                     # Entry point: runs full ETL + model training pipeline
```
---

## Documentation Links

- **Video Processing & Data Upload:**  
  [video-ingestion README](https://github.com/Onyx-AI-LLC/Human-Alignment-Hazardous-Driving-Detection/blob/main/video-ingestion/README.md)

- **Driving Simulation Web Application:**  
  - [Frontend README](https://github.com/Onyx-AI-LLC/Human-Alignment-Hazardous-Driving-Detection/blob/main/frontend/README.md)  
  - [Backend README](https://github.com/Onyx-AI-LLC/Human-Alignment-Hazardous-Driving-Detection/blob/main/server/README.md)

- **ETL Pipeline:**  
  [ETL README](https://github.com/Onyx-AI-LLC/Human-Alignment-Hazardous-Driving-Detection/blob/main/etl/README.md)

- **Exploratory Data Analysis:**  
  [EDA README](https://github.com/Onyx-AI-LLC/Human-Alignment-Hazardous-Driving-Detection/blob/main/eda/README.md)

---

## Getting Started

### Step 1: Clone the Repository

```bash
git clone https://github.com/Onyx-AI-LLC/Human-Alignment-Hazardous-Driving-Detection.git
cd Human-Alignment-Hazardous-Driving-Detection
```

### Step 2: Create and Activate a Virtual Environment

On macOS/Linux:

```bash
python3 -m venv venv
source venv/bin/activate
```

On Windows:

```bash
python -m venv venv
venv\Scripts\activate
```

### Step 3: Install Required Dependencies

```bash 
pip install -r requirements.txt
```

### Step 4: Configure Environment Variables

Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
```

Required variables:
- `MONGO_URI` — MongoDB connection string
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` — AWS credentials for S3
- `S3_BUCKET_NAME` — S3 bucket containing raw driving footage

### Step 5: Run the Pipeline

```bash
python main.py
```

This runs the full ETL pipeline followed by naive, traditional CV, and deep learning model training.

---

**This research is made possible due to collaboration between Duke University & Onyx AI LLC.**
