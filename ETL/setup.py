#!/usr/bin/env python3
"""
Master Setup File for HAHD Preprocessing Pipeline
=================================================

This is the master orchestrator file that runs the complete preprocessing pipeline
in the exact order specified in the EDAf.ipynb notebook. It can be executed both 
locally and in AWS Lambda/EventBridge environments.

The pipeline consists of 4 sequential steps:
1. screensize_attention - Conservative gaze preprocessing with viewport normalization
2. render_delay - Extract last 15 seconds to remove render delays
3. reaction_time - Apply demographic-based reaction time corrections
4. structure - Create timestamp-level training dataset with 50+ features

Each step loads output from the previous step and saves results to S3 in 100-row batches.
The pipeline ends at the structure step (no bounding box processing) and outputs
one consolidated CSV file at the end.

Usage:
- Local: python setup.py
- AWS Lambda: This file is called by EventBridge every 15 minutes
"""

import os
import sys
import traceback
import time
from datetime import datetime
from pathlib import Path
import pandas as pd

# Add current directory to Python path for imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Import preprocessing utilities
from preprocessing_utils import get_preprocessing_utils

# Import individual processing modules
import preprocess_screensize_attention
import preprocess_render_delay  
import preprocess_reaction_time
import preprocess_structure
import download_s3_data
import subprocess
import glob
import json

def detect_environment():
    """Detect if running locally or in AWS Lambda"""
    is_aws = bool(os.environ.get('AWS_LAMBDA_FUNCTION_NAME') or 
                  os.environ.get('AWS_EXECUTION_ENV'))
    return 'AWS Lambda' if is_aws else 'Local'

def setup_local_directories():
    """Create local output directories when running locally"""
    # Get project root (parent of etl directory)
    project_root = Path(__file__).parent.parent
    
    local_dirs = [
        # Silver directory structure (mirrors S3)
        project_root / 'data' / 'silver',
        project_root / 'data' / 'silver' / 'results',
        project_root / 'data' / 'silver' / 'users',
        project_root / 'data' / 'silver' / 'csv',
        
        # Logs directory
        project_root / 'logs'
    ]
    
    for dir_path in local_dirs:
        dir_path.mkdir(parents=True, exist_ok=True)
    
    return local_dirs

def save_to_local_silver(df, script_name, data_type='results', batch_size=100):
    """Save DataFrame to local silver directory structure in batches"""
    if df.empty:
        return []
    
    # Get project root (parent of etl directory)
    project_root = Path(__file__).parent.parent
    
    # Determine output path
    if data_type == 'users':
        silver_path = project_root / 'data' / 'silver' / 'users'
    else:
        silver_path = project_root / 'data' / 'silver' / 'results'
    
    silver_path.mkdir(parents=True, exist_ok=True)
    
    saved_files = []
    
    # Split into batches
    total_batches = (len(df) + batch_size - 1) // batch_size
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, len(df))
        batch_df = df.iloc[start_idx:end_idx]
        
        # Generate filename (matches S3 structure)
        filename = f"{script_name}_batch_{batch_idx:04d}.parquet"
        file_path = silver_path / filename
        
        # Save as parquet to match S3 format
        batch_df.to_parquet(file_path, index=False)
        saved_files.append(str(file_path))
    
    return saved_files

def load_from_local_silver(script_name, data_type='results'):
    """Load DataFrame from local silver directory batches"""
    # Get project root (parent of etl directory)
    project_root = Path(__file__).parent.parent
    
    if data_type == 'users':
        silver_path = project_root / 'data' / 'silver' / 'users'
    else:
        silver_path = project_root / 'data' / 'silver' / 'results'
    
    if not silver_path.exists():
        return pd.DataFrame()
    
    # Find matching files
    matching_files = list(silver_path.glob(f"{script_name}_batch_*.parquet"))
    
    if not matching_files:
        return pd.DataFrame()
    
    # Load all batches and combine
    dataframes = []
    for file_path in sorted(matching_files):
        df = pd.read_parquet(file_path)
        dataframes.append(df)
    
    if dataframes:
        return pd.concat(dataframes, ignore_index=True)
    else:
        return pd.DataFrame()

def check_local_data_sync(utils):
    """Check if local RAW data is synchronized with S3 raw data. Only for local execution."""
    utils.log_progress("Checking local raw data synchronization with S3...")
    
    # Get project root
    project_root = Path(__file__).parent.parent
    
    try:
        # Count local RAW files only
        local_results_dir = project_root / 'data' / 'raw' / 'results'
        local_users_csv = project_root / 'data' / 'raw' / 'users_data_raw.csv'
        local_videos_dir = project_root / 'data' / 'raw' / 'videos'
        
        local_results_count = 0
        if local_results_dir.exists():
            local_results_count = len(list(local_results_dir.glob('*.json')))
        
        local_users_count = 0
        if local_users_csv.exists():
            local_users_df = pd.read_csv(local_users_csv)
            local_users_count = len(local_users_df)
        
        local_videos_count = 0
        if local_videos_dir.exists():
            local_videos_count = len(list(local_videos_dir.glob('*.mp4'))) + len(list(local_videos_dir.glob('*.avi'))) + len(list(local_videos_dir.glob('*.mov')))
        
        utils.log_progress(f"Local raw data counts: {local_results_count} results, {local_users_count} users, {local_videos_count} videos")
        
        # Count S3 RAW files only
        s3_results_count = 0
        s3_users_count = 0
        s3_videos_count = 0
        
        # Check S3 raw/results/
        response = utils.s3.list_objects_v2(Bucket=utils.bucket, Prefix='raw/results/')
        if 'Contents' in response:
            s3_results_count = len([obj for obj in response['Contents'] if obj['Key'].endswith('.json')])
        
        # Check S3 raw/users/  
        response = utils.s3.list_objects_v2(Bucket=utils.bucket, Prefix='raw/users/')
        if 'Contents' in response:
            s3_users_count = len([obj for obj in response['Contents'] if obj['Key'].endswith('.json')])
        
        # Check S3 raw/videos/ (limited check to avoid long listing)
        response = utils.s3.list_objects_v2(Bucket=utils.bucket, Prefix='raw/videos/', MaxKeys=1000)
        if 'Contents' in response:
            s3_videos_count = len([obj for obj in response['Contents'] if obj['Key'].endswith(('.mp4', '.avi', '.mov'))])
        
        utils.log_progress(f"S3 raw data counts: {s3_results_count} results, {s3_users_count} users, {s3_videos_count}+ videos")
        
        # Compare ONLY raw data counts
        results_synced = (local_results_count == s3_results_count and local_results_count > 0)
        users_synced = (local_users_count == s3_users_count and local_users_count > 0)
        videos_synced = (local_videos_count >= min(s3_videos_count, 10))  # Accept if we have at least 10 videos locally
        
        if results_synced and users_synced and videos_synced:
            utils.log_progress("Local raw data is synchronized with S3")
            return True
        else:
            utils.log_progress("Local raw data is NOT synchronized with S3")
            utils.log_progress(f"Results synced: {results_synced}, Users synced: {users_synced}, Videos synced: {videos_synced}")
            return False
            
    except Exception as e:
        utils.log_error("Error checking local raw data sync", e)
        return False

def sync_local_data_with_s3(utils):
    """Run download_s3_data.py to synchronize local data with S3"""
    utils.log_progress("Synchronizing local data with S3...")
    
    try:
        # Import and run the download script
        utils.log_progress("Running download_s3_data.py...")
        
        # Run the download script in a subprocess to avoid user input issues
        # Create a modified version that skips video download prompt
        modified_main_code = '''
import download_s3_data
import boto3

def automated_download():
    """Automated version of download_s3_data.main() for pipeline use"""
    print("Starting automated S3 data download...")
    
    # Setup directories
    download_s3_data.setup_directories()
    
    # Initialize S3 client
    try:
        s3_client = boto3.client('s3')
        print("Success: Connected to S3")
    except Exception as e:
        print(f"Error: Failed to connect to S3: {e}")
        return False
    
    # Download data
    bucket_name = 'hahd-primary-data-storage'
    
    results_count = download_s3_data.download_s3_results(s3_client, bucket_name)
    users_count = download_s3_data.download_s3_users(s3_client, bucket_name)
    
    # Skip video download for automated pipeline - just download first 10 for testing
    print("Downloading first 10 videos for preprocessing pipeline...")
    videos_count = download_s3_data.download_s3_videos(s3_client, bucket_name, limit=10)
    
    # Create CSV files
    csv_results, csv_users = download_s3_data.create_local_csv_files(s3_client, bucket_name)
    
    print(f"Download complete: {results_count} results, {users_count} users, {videos_count} videos")
    print(f"CSV files: {csv_results} result records, {csv_users} user records")
    
    return csv_results > 0 and csv_users > 0

if __name__ == "__main__":
    automated_download()
'''
        
        # Write temporary automated download script
        temp_script = Path(__file__).parent / 'temp_automated_download.py'
        with open(temp_script, 'w') as f:
            f.write(modified_main_code)
        
        # Run the automated download
        result = subprocess.run([
            sys.executable, str(temp_script)
        ], capture_output=True, text=True, timeout=600)  # 10 minute timeout
        
        # Clean up temp script
        if temp_script.exists():
            temp_script.unlink()
        
        if result.returncode == 0:
            utils.log_progress("Successfully synchronized local data with S3")
            utils.log_progress("Download output:")
            for line in result.stdout.split('\n')[-10:]:  # Last 10 lines
                if line.strip():
                    utils.log_progress(f"  {line}")
            return True
        else:
            utils.log_error("Failed to synchronize local data with S3")
            utils.log_error(f"Error output: {result.stderr}")
            return False
            
    except Exception as e:
        utils.log_error("Error running S3 data synchronization", e)
        return False

def setup_pipeline_logging():
    """Initialize logging for the master pipeline"""
    utils = get_preprocessing_utils()
    environment = detect_environment()
    
    # Setup local directories if running locally
    if environment == 'Local':
        setup_local_directories()
    
    utils.log_progress("=" * 80)
    utils.log_progress("HAHD PREPROCESSING PIPELINE - MASTER SETUP")
    utils.log_progress("=" * 80)
    utils.log_progress(f"Environment: {environment}")
    utils.log_progress(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    utils.log_progress(f"Pipeline steps: screensize_attention → render_delay → reaction_time → structure")
    
    if environment == 'Local':
        utils.log_progress("Local mode: Data will be saved to ./data/silver/")
        
        # Check local data synchronization
        utils.log_progress("\n" + "=" * 60)
        utils.log_progress("LOCAL DATA SYNCHRONIZATION CHECK")
        utils.log_progress("=" * 60)
        
        is_synced = check_local_data_sync(utils)
        
        if not is_synced:
            utils.log_progress("Local data is not synchronized. Running download_s3_data.py...")
            sync_success = sync_local_data_with_s3(utils)
            
            if not sync_success:
                utils.log_error("Failed to synchronize local data with S3")
                utils.log_error("Cannot proceed with local preprocessing - raw data required")
                return None, environment
            
            # Verify synchronization after download
            utils.log_progress("Verifying data synchronization after download...")
            is_synced = check_local_data_sync(utils)
            
            if not is_synced:
                utils.log_error("Data synchronization verification failed")
                utils.log_error("Cannot proceed with preprocessing - please check data integrity")
                return None, environment
        
        utils.log_progress("Local data is ready for preprocessing")
    else:
        utils.log_progress(f"AWS mode: Data will be saved to S3 bucket: {utils.bucket}")
        utils.log_progress("AWS mode: Raw data will be loaded directly from S3")
    
    return utils, environment

def run_step_1_screensize_attention(utils, environment):
    """Step 1: Conservative gaze preprocessing with viewport normalization"""
    utils.log_progress("\n" + "=" * 60)
    utils.log_progress("STEP 1: SCREENSIZE ATTENTION PREPROCESSING")
    utils.log_progress("=" * 60)
    utils.log_progress("Processing: Raw survey data → Normalized gaze coordinates")
    
    try:
        # Set environment variable for the current script
        os.environ['CURRENT_SCRIPT'] = 'screensize_attention'
        
        if environment == 'Local':
            # For local execution, load data from local JSON files (same as S3 structure)
            utils.log_progress("Local mode: Loading data from local JSON files")
            project_root = Path(__file__).parent.parent
            local_results_dir = project_root / 'data' / 'raw' / 'results'
            
            if local_results_dir.exists():
                utils.log_progress(f"Loading local survey results from: {local_results_dir}")
                
                # Load and process JSON files (same way preprocessing_utils does for S3)
                raw_data = []
                json_files = list(local_results_dir.glob('*.json'))
                
                if not json_files:
                    utils.log_error(f"No JSON files found in {local_results_dir}")
                    return False
                
                utils.log_progress(f"Found {len(json_files)} JSON files to process")
                
                for json_file in json_files:
                    try:
                        with open(json_file, 'r') as f:
                            json_data = json.load(f)
                        # Normalize JSON to DataFrame format (same as pandas.json_normalize)
                        normalized_data = pd.json_normalize([json_data])
                        raw_data.append(normalized_data)
                    except Exception as e:
                        utils.log_progress(f"Error loading {json_file}: {e}")
                        continue
                
                if raw_data:
                    raw_df = pd.concat(raw_data, ignore_index=True)
                    utils.log_progress(f"Loaded {len(raw_df)} records from local JSON files")
                    
                    # Use the preprocessor directly with JSON-normalized data
                    from preprocess_screensize_attention import ConservativeGazePreprocessor
                    preprocessor = ConservativeGazePreprocessor(
                        video_aspect_ratio=1280/960,
                        quality_threshold=0.25
                    )
                    result_df = preprocessor.process_dataset(raw_df)
                else:
                    utils.log_error("No valid JSON data loaded")
                    return False
            else:
                utils.log_error(f"Local results directory not found: {local_results_dir}")
                return False
            
            if not result_df.empty:
                # Save to processed directory
                project_root = Path(__file__).parent.parent
                local_output = project_root / 'data' / 'silver' / 'csv' / 'step1_screensize_attention_processed.csv'
                result_df.to_csv(local_output, index=False)
                utils.log_progress(f"Step 1 CSV saved to silver/csv/: {local_output}")
                
                # Save to local silver directory (mirroring S3 structure)
                silver_files = save_to_local_silver(result_df, 'screensize_attention', 'results')
                utils.log_progress(f"Step 1 output saved to silver: {len(silver_files)} batch files")
        else:
            # For AWS execution, use S3 (original behavior)
            result_df = preprocess_screensize_attention.main()
        
        if result_df.empty:
            utils.log_error("Step 1 failed: No data returned from screensize_attention preprocessing")
            return False
            
        utils.log_progress(f"Step 1 COMPLETE: Processed {len(result_df)} records")
        utils.log_progress("Output: Gaze data normalized to common viewport with 25% quality filtering")
        return True
        
    except Exception as e:
        utils.log_error("Step 1 FAILED: Error in screensize_attention preprocessing", e)
        return False

def run_step_2_render_delay(utils, environment):
    """Step 2: Extract last 15 seconds to remove render delays"""
    utils.log_progress("\n" + "=" * 60)
    utils.log_progress("STEP 2: RENDER DELAY PREPROCESSING")  
    utils.log_progress("=" * 60)
    utils.log_progress("Processing: Normalized data → Last 15 seconds extracted")
    
    try:
        # Set environment variable for the current script
        os.environ['CURRENT_SCRIPT'] = 'render_delay'
        
        # Load previous step output
        if environment == 'Local':
            # Try to load from local silver first, then processed, then S3
            input_df = load_from_local_silver('screensize_attention', 'results')
            if not input_df.empty:
                utils.log_progress(f"Loading Step 1 output from local silver: {len(input_df)} records")
            else:
                project_root = Path(__file__).parent.parent
                local_input = project_root / 'data' / 'silver' / 'csv' / 'step1_screensize_attention_processed.csv'
                if local_input.exists():
                    utils.log_progress(f"Loading Step 1 output from processed file: {local_input}")
                    input_df = pd.read_csv(local_input)
                else:
                    utils.log_progress("Local files not found, loading from S3")
                    input_df = utils.load_previous_output('screensize_attention', 'results')
        else:
            input_df = utils.load_previous_output('screensize_attention', 'results')
            
        if input_df.empty:
            utils.log_error("Step 2 failed: No input data from screensize_attention step")
            return False
            
        utils.log_progress(f"Loaded {len(input_df)} records from Step 1")
        
        # Create temporary input file
        temp_input = "/tmp/step2_input.csv"
        input_df.to_csv(temp_input, index=False)
        
        # Process through render delay extraction
        processed_df = preprocess_render_delay.extract_last_15_seconds(
            temp_input, 
            "/tmp/step2_output.csv", 
            duration_seconds=15.0
        )
        
        if environment == 'Local':
            # Save to processed directory
            project_root = Path(__file__).parent.parent
            local_output = project_root / 'data' / 'silver' / 'csv' / 'step2_render_delay_processed.csv'
            processed_df.to_csv(local_output, index=False)
            utils.log_progress(f"Step 2 CSV saved to silver/csv/: {local_output}")
            
            # Save to local silver directory (mirroring S3 structure)
            silver_files = save_to_local_silver(processed_df, 'render_delay', 'results')
            utils.log_progress(f"Step 2 output saved to silver: {len(silver_files)} batch files")
        else:
            # Save to unified S3 structure
            saved_keys = utils.save_unified_output(
                processed_df,
                data_type='results', 
                script_name='render_delay'
            )
            
            if not saved_keys:
                utils.log_error("Step 2 failed: Could not save output to S3")
                return False
            
        utils.log_progress(f"Step 2 COMPLETE: Processed {len(processed_df)} records")
        utils.log_progress("Output: Videos truncated to last 15 seconds to remove render delays")
        return True
        
    except Exception as e:
        utils.log_error("Step 2 FAILED: Error in render_delay preprocessing", e)
        return False

def run_step_3_reaction_time(utils, environment):
    """Step 3: Apply demographic-based reaction time corrections"""
    utils.log_progress("\n" + "=" * 60)
    utils.log_progress("STEP 3: REACTION TIME PREPROCESSING")
    utils.log_progress("=" * 60) 
    utils.log_progress("Processing: 15-second data → Reaction time adjusted timestamps")
    
    try:
        # Set environment variable for the current script
        os.environ['CURRENT_SCRIPT'] = 'reaction_time'
        
        # Load previous step output
        if environment == 'Local':
            # Try to load from local silver first, then processed, then S3
            gaze_df = load_from_local_silver('render_delay', 'results')
            if not gaze_df.empty:
                utils.log_progress(f"Loading Step 2 output from local silver: {len(gaze_df)} records")
            else:
                project_root = Path(__file__).parent.parent
                local_input = project_root / 'data' / 'silver' / 'csv' / 'step2_render_delay_processed.csv'
                if local_input.exists():
                    utils.log_progress(f"Loading Step 2 output from processed file: {local_input}")
                    gaze_df = pd.read_csv(local_input)
                else:
                    utils.log_progress("Local files not found, loading from S3")
                    gaze_df = utils.load_previous_output('render_delay', 'results')
        else:
            gaze_df = utils.load_previous_output('render_delay', 'results')
            
        if gaze_df.empty:
            utils.log_error("Step 3 failed: No input data from render_delay step")
            return False
            
        utils.log_progress(f"Loaded {len(gaze_df)} records from Step 2")
        
        # Load user demographics for reaction time calculations
        if environment == 'Local':
            # Load user demographics from local JSON files
            project_root = Path(__file__).parent.parent
            local_users_dir = project_root / 'data' / 'raw' / 'users'
            
            if local_users_dir.exists():
                utils.log_progress(f"Loading local user data from: {local_users_dir}")
                
                # Load and process JSON files (same way preprocessing_utils does for S3)
                user_data = []
                json_files = list(local_users_dir.glob('*.json'))
                
                if json_files:
                    utils.log_progress(f"Found {len(json_files)} user JSON files to process")
                    
                    for json_file in json_files:
                        try:
                            with open(json_file, 'r') as f:
                                json_data = json.load(f)
                            # Normalize JSON to DataFrame format
                            normalized_data = pd.json_normalize([json_data])
                            user_data.append(normalized_data)
                        except Exception as e:
                            utils.log_progress(f"Error loading {json_file}: {e}")
                            continue
                    
                    if user_data:
                        users_df = pd.concat(user_data, ignore_index=True)
                        utils.log_progress(f"Loaded {len(users_df)} user records from local JSON files")
                    else:
                        utils.log_progress("No valid user JSON data loaded")
                        users_df = pd.DataFrame()
                else:
                    utils.log_progress(f"No user JSON files found in {local_users_dir}")
                    users_df = pd.DataFrame()
            else:
                utils.log_progress(f"Local user directory not found: {local_users_dir}")
                users_df = pd.DataFrame()
        else:
            users_df = utils.download_raw_data('users')
        
        if users_df.empty:
            utils.log_progress("WARNING: No user demographics found, skipping reaction time adjustments")
            processed_df = gaze_df
        else:
            utils.log_progress(f"Loaded {len(users_df)} user demographic records")
            
            # Create temporary files for processing
            temp_gaze = "/tmp/step3_gaze.csv"
            temp_users = "/tmp/step3_users.csv"
            temp_output = "/tmp/step3_output.csv"
            
            gaze_df.to_csv(temp_gaze, index=False)
            users_df.to_csv(temp_users, index=False)
            
            # Apply reaction time adjustments using Age × 2.8ms - 50ms (if male) formula
            processed_df = preprocess_reaction_time.apply_reaction_time_adjustments(
                temp_gaze, temp_users, temp_output
            )
        
        if environment == 'Local':
            # Save to processed directory
            project_root = Path(__file__).parent.parent
            local_output = project_root / 'data' / 'silver' / 'csv' / 'step3_reaction_time_processed.csv'
            processed_df.to_csv(local_output, index=False)
            utils.log_progress(f"Step 3 CSV saved to silver/csv/: {local_output}")
            
            # Save to local silver directory (mirroring S3 structure)
            silver_files = save_to_local_silver(processed_df, 'reaction_time', 'results')
            utils.log_progress(f"Step 3 output saved to silver: {len(silver_files)} batch files")
        else:
            # Save to unified S3 structure  
            saved_keys = utils.save_unified_output(
                processed_df,
                data_type='results',
                script_name='reaction_time'  
            )
            
            if not saved_keys:
                utils.log_error("Step 3 failed: Could not save output to S3")
                return False
            
        utils.log_progress(f"Step 3 COMPLETE: Processed {len(processed_df)} records")
        utils.log_progress("Output: Spacebar timestamps adjusted for demographic-based reaction times")
        return True
        
    except Exception as e:
        utils.log_error("Step 3 FAILED: Error in reaction_time preprocessing", e)
        return False

def run_step_4_structure(utils, environment):
    """Step 4: Create timestamp-level training dataset with 50+ features"""
    utils.log_progress("\n" + "=" * 60)
    utils.log_progress("STEP 4: STRUCTURE PREPROCESSING (FINAL)")
    utils.log_progress("=" * 60)
    utils.log_progress("Processing: Adjusted data → Timestamp-level training features")
    
    try:
        # Set environment variable for the current script  
        os.environ['CURRENT_SCRIPT'] = 'structure'
        
        # Load previous step output
        if environment == 'Local':
            # Try to load from local silver first, then processed, then S3
            input_df = load_from_local_silver('reaction_time', 'results')
            if not input_df.empty:
                utils.log_progress(f"Loading Step 3 output from local silver: {len(input_df)} records")
            else:
                project_root = Path(__file__).parent.parent
                local_input = project_root / 'data' / 'silver' / 'csv' / 'step3_reaction_time_processed.csv'
                if local_input.exists():
                    utils.log_progress(f"Loading Step 3 output from processed file: {local_input}")
                    input_df = pd.read_csv(local_input)
                else:
                    utils.log_progress("Local files not found, loading from S3")
                    input_df = utils.load_previous_output('reaction_time', 'results')
        else:
            input_df = utils.load_previous_output('reaction_time', 'results')
            
        if input_df.empty:
            utils.log_error("Step 4 failed: No input data from reaction_time step")
            return False
            
        utils.log_progress(f"Loaded {len(input_df)} records from Step 3")
        
        # Create temporary input file for heavy compute processing
        temp_input = "/tmp/step4_input.csv"
        temp_output = "/tmp/step4_output.csv"
        input_df.to_csv(temp_input, index=False)
        
        # Process through structure creation (heavy compute step)
        utils.log_progress("Starting structure processing (this may take several minutes for large datasets)")
        result_df = preprocess_structure.process_gaze_data(temp_input, temp_output)
        
        if environment == 'Local':
            # Save to processed directory
            project_root = Path(__file__).parent.parent
            local_output = project_root / 'data' / 'silver' / 'csv' / 'step4_structure_processed.csv'
            result_df.to_csv(local_output, index=False)
            utils.log_progress(f"Step 4 CSV saved to silver/csv/: {local_output}")
            
            # Save to local silver directory (mirroring S3 structure)
            silver_files = save_to_local_silver(result_df, 'structure', 'results')
            utils.log_progress(f"Step 4 output saved to silver: {len(silver_files)} batch files")
        else:
            # Save to unified S3 structure
            saved_keys = utils.save_unified_output(
                result_df,
                data_type='results', 
                script_name='structure'
            )
            
            if not saved_keys:
                utils.log_error("Step 4 failed: Could not save output to S3")
                return False
            
        utils.log_progress(f"Step 4 COMPLETE: Created {len(result_df)} training samples")
        utils.log_progress(f"Output: Timestamp-level dataset with {len(result_df.columns)} features")
        return True
        
    except Exception as e:
        utils.log_error("Step 4 FAILED: Error in structure preprocessing", e)
        return False

def create_final_consolidated_csv(utils, environment):
    """Create one final consolidated CSV from the structure output"""
    utils.log_progress("\n" + "=" * 60)
    utils.log_progress("FINAL STEP: CREATING CONSOLIDATED OUTPUT CSV")
    utils.log_progress("=" * 60)
    
    try:
        # Load all structure output files and combine into one CSV
        if environment == 'Local':
            # Try to load from local silver first, then processed, then S3
            final_df = load_from_local_silver('structure', 'results')
            if not final_df.empty:
                utils.log_progress(f"Loading final data from local silver: {len(final_df)} records")
            else:
                project_root = Path(__file__).parent.parent
                local_input = project_root / 'data' / 'silver' / 'csv' / 'step4_structure_processed.csv'
                if local_input.exists():
                    utils.log_progress(f"Loading final data from processed file: {local_input}")
                    final_df = pd.read_csv(local_input)
                else:
                    utils.log_progress("Local files not found, loading from S3")
                    final_df = utils.load_previous_output('structure', 'results')
        else:
            final_df = utils.load_previous_output('structure', 'results')
        
        if final_df.empty:
            utils.log_error("Final consolidation failed: No data from structure step")
            return False
            
        # Create final consolidated CSV
        if environment == 'Local':
            # Save locally
            project_root = Path(__file__).parent.parent
            final_csv_path = project_root / 'data' / 'silver' / 'csv' / 'hahd_preprocessed_final.csv'
            final_df.to_csv(final_csv_path, index=False)
            utils.log_progress(f"FINAL CSV CREATED: {final_csv_path}")
        else:
            # Save to S3
            final_csv_path = "/tmp/hahd_preprocessed_final.csv"
            final_df.to_csv(final_csv_path, index=False)
            
            # Upload final CSV to S3
            s3_key = "silver/results/hahd_preprocessed_final.csv"
            utils.s3.upload_file(final_csv_path, utils.bucket, s3_key)
            utils.log_progress(f"FINAL CSV CREATED: s3://{utils.bucket}/{s3_key}")
        
        utils.log_progress(f"Final dataset: {len(final_df)} samples with {len(final_df.columns)} features")
        
        return True
        
    except Exception as e:
        utils.log_error("Final consolidation FAILED", e)
        return False

def main():
    """Main pipeline execution function"""
    start_time = time.time()
    
    try:
        # Initialize logging and environment detection
        setup_result = setup_pipeline_logging()
        if setup_result[0] is None:
            print("Setup failed - cannot proceed with pipeline")
            return False
        utils, environment = setup_result
        
        # Run preprocessing pipeline in sequence
        steps_completed = 0
        
        # Step 1: Screensize Attention Preprocessing
        if run_step_1_screensize_attention(utils, environment):
            steps_completed += 1
        else:
            utils.log_error("Pipeline FAILED at Step 1 - Cannot continue")
            return False
            
        # Step 2: Render Delay Preprocessing  
        if run_step_2_render_delay(utils, environment):
            steps_completed += 1
        else:
            utils.log_error("Pipeline FAILED at Step 2 - Cannot continue")
            return False
            
        # Step 3: Reaction Time Preprocessing
        if run_step_3_reaction_time(utils, environment):
            steps_completed += 1
        else:
            utils.log_error("Pipeline FAILED at Step 3 - Cannot continue") 
            return False
            
        # Step 4: Structure Preprocessing (Final)
        if run_step_4_structure(utils, environment):
            steps_completed += 1
        else:
            utils.log_error("Pipeline FAILED at Step 4 - Cannot continue")
            return False
            
        # Create final consolidated CSV
        if create_final_consolidated_csv(utils, environment):
            steps_completed += 1
        else:
            utils.log_error("Pipeline FAILED at final consolidation")
            return False
            
        # Pipeline completion
        end_time = time.time()
        total_duration = end_time - start_time
        
        utils.log_progress("\n" + "=" * 80)
        utils.log_progress("HAHD PREPROCESSING PIPELINE COMPLETED SUCCESSFULLY!")
        utils.log_progress("=" * 80)
        utils.log_progress(f"Total steps completed: {steps_completed}/5")
        utils.log_progress(f"Total execution time: {total_duration/60:.2f} minutes")
        utils.log_progress(f"Environment: {detect_environment()}")
        utils.log_progress(f"Final output: Ready for training in silver/results/")
        utils.log_progress("Next steps: The processed data is now ready for model training")
        
        # Upload final progress log
        utils.upload_progress_log()
        
        return True
        
    except Exception as e:
        utils.log_error("CRITICAL PIPELINE FAILURE", e)
        utils.upload_progress_log()  # Upload logs even on failure
        return False

if __name__ == "__main__":
    success = main()
    exit_code = 0 if success else 1
    
    print(f"\nPipeline finished with exit code: {exit_code}")
    sys.exit(exit_code)