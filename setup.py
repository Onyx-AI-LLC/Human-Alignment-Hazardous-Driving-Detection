#!/usr/bin/env python3
"""
HAHD Setup Script
Downloads S3 data and runs complete local preprocessing pipeline
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

def run_command(command, description):
    """Run a command and handle output"""
    print(f"\n {description}")
    print(f"Running: {command}")
    
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: Command failed: {e}")
        if e.stdout:
            print("STDOUT:", e.stdout)
        if e.stderr:
            print("STDERR:", e.stderr)
        return False

def check_dependencies():
    """Check if required Python packages are installed"""
    print(" Checking dependencies...")
    
    required_packages = [
        'boto3',
        'pandas', 
        'numpy',
        'ultralytics',
        'opencv-python',
        'scikit-learn',
        'matplotlib'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"Success: {package}")
        except ImportError:
            print(f"Error: {package} - MISSING")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n Installing missing packages: {', '.join(missing_packages)}")
        install_cmd = f"pip install {' '.join(missing_packages)}"
        if not run_command(install_cmd, "Installing missing packages"):
            print("Error: Failed to install dependencies")
            return False
    
    return True

def setup_directories():
    """Create necessary directories"""
    print("\n Setting up directory structure...")
    
    directories = [
        'data/raw',
        'data/processed',
        'data/yolo_hazard_output',
        'models'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"Success: {directory}")

def download_s3_data():
    """Download data from S3"""
    print("\n Downloading data from S3...")
    
    download_script = "etl/download_s3_data.py"
    if not Path(download_script).exists():
        print(f"Error: Download script not found: {download_script}")
        return False
    
    return run_command(f"python {download_script}", "Downloading S3 data")

def run_preprocessing():
    """Run the complete preprocessing pipeline"""
    print("\n Running preprocessing pipeline...")
    
    preprocessing_script = "etl/run_local_preprocessing.py"
    if not Path(preprocessing_script).exists():
        print(f"Error: Preprocessing script not found: {preprocessing_script}")
        return False
    
    return run_command(f"python {preprocessing_script}", "Running preprocessing pipeline")

def verify_outputs():
    """Verify that expected output files were created"""
    print("\nSuccess: Verifying outputs...")
    
    expected_files = [
        'data/raw/survey_results_raw.csv',
        'data/raw/users_data_raw.csv', 
        'data/processed/hazard_training_dataset.csv',
        'data/processed/local_preprocessing_summary.json'
    ]
    
    optional_files = [
        'data/processed/hazard_training_final.csv',
        'models/hazard_detection_model.pkl',
        'data/yolo_hazard_output/'
    ]
    
    all_good = True
    
    for file_path in expected_files:
        if Path(file_path).exists():
            if file_path.endswith('.csv'):
                # Get row count for CSV files
                try:
                    import pandas as pd
                    df = pd.read_csv(file_path)
                    print(f"Success: {file_path} ({len(df):,} rows)")
                except:
                    print(f"Success: {file_path}")
            else:
                print(f"Success: {file_path}")
        else:
            print(f"Error: {file_path} - MISSING")
            all_good = False
    
    for file_path in optional_files:
        if Path(file_path).exists():
            if file_path.endswith('.csv'):
                try:
                    import pandas as pd
                    df = pd.read_csv(file_path)
                    print(f"Success: {file_path} ({len(df):,} rows)")
                except:
                    print(f"Success: {file_path}")
            else:
                print(f"Success: {file_path}")
        else:
            print(f"Warning: {file_path} - Optional (not created)")
    
    return all_good

def main():
    """Main setup function"""
    print("Starting: HAHD Setup - Starting complete data pipeline setup")
    print(f" {datetime.now().isoformat()}")
    print("="*60)
    
    # Check dependencies first
    if not check_dependencies():
        print("\nError: Setup failed: Missing dependencies")
        return False
    
    # Setup directories
    setup_directories()
    
    # Download S3 data
    if not download_s3_data():
        print("\nError: Setup failed: S3 download failed")
        return False
    
    # Run preprocessing pipeline
    if not run_preprocessing():
        print("\nError: Setup failed: Preprocessing failed")
        return False
    
    # Verify outputs
    if not verify_outputs():
        print("\nWarning: Setup completed with warnings: Some files missing")
    else:
        print("\nComplete: Setup completed successfully!")
    
    print("\n Next steps:")
    print("1. Check data/processed/hazard_training_final.csv for your training dataset")
    print("2. Check models/hazard_detection_model.pkl for your trained model")
    print("3. Review data/processed/local_preprocessing_summary.json for pipeline details")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)