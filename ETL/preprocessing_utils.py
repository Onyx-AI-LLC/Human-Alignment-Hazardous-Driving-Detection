#!/usr/bin/env python3
"""
Unified preprocessing utilities for HAHD pipeline.
Handles S3 I/O, parquet formatting, batch processing, and detailed logging.
"""

import os
import json
import pandas as pd
import boto3
import logging
import traceback
import time
from datetime import datetime
from typing import Tuple, List, Optional
import numpy as np
from pathlib import Path

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)


class PreprocessingUtils:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.s3 = boto3.client('s3')
        self.bucket = os.environ.get('BUCKET_NAME', 'hahd-primary-data-storage')
        self.batch_size = int(os.environ.get('BATCH_SIZE', '100'))
        self.output_format = os.environ.get('OUTPUT_FORMAT', 'parquet')
        
        # Unified output paths
        self.silver_users_path = f"s3://{self.bucket}/silver/users/"
        self.silver_results_path = f"s3://{self.bucket}/silver/results/"
        
        # Progress tracking
        self.current_script = os.environ.get('CURRENT_SCRIPT', 'unknown')
        self.progress_file = '/tmp/progress.log'
        
        self.logger.info(f"PreprocessingUtils initialized - Script: {self.current_script}")
        self.logger.info(f"Config - Bucket: {self.bucket}, Batch size: {self.batch_size}, Format: {self.output_format}")
    
    def log_progress(self, message: str, current: int = None, total: int = None):
        """Log progress with row counts and save to progress file"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if current is not None and total is not None:
            progress_msg = f"[{self.current_script}] {message} - Progress: {current}/{total} ({current/total*100:.1f}%)"
        else:
            progress_msg = f"[{self.current_script}] {message}"
        
        self.logger.info(progress_msg)
        
        # Save to progress file for external monitoring
        try:
            with open(self.progress_file, 'a') as f:
                f.write(f"{timestamp}: {progress_msg}\n")
        except Exception as e:
            self.logger.error(f"Failed to write progress file: {e}")
    
    def log_error(self, message: str, exception: Exception = None):
        """Log errors with full traceback"""
        self.logger.error(f"[{self.current_script}] ERROR: {message}")
        if exception:
            self.logger.error(f"Exception details: {str(exception)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Save to progress file for external monitoring
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(self.progress_file, 'a') as f:
                f.write(f"{timestamp}: [ERROR] {message}\n")
                if exception:
                    f.write(f"{timestamp}: Exception: {str(exception)}\n")
        except Exception as e:
            self.logger.error(f"Failed to write error to progress file: {e}")
        
    def download_raw_data(self, data_type: str = 'results') -> pd.DataFrame:
        """
        Download raw data from S3 with detailed progress tracking.
        
        Args:
            data_type: 'results' or 'users'
            
        Returns:
            Combined DataFrame from all raw files
        """
        try:
            self.log_progress(f"Starting download of raw {data_type} data from S3")
            
            prefix = f"raw/{data_type}/"
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            
            if 'Contents' not in response:
                self.log_error(f"No files found in {prefix}")
                return pd.DataFrame()
            
            files = [obj for obj in response['Contents'] if obj['Key'].endswith('.json')]
            total_files = len(files)
            self.log_progress(f"Found {total_files} JSON files to process")
            
            dataframes = []
            for i, obj in enumerate(files):
                try:
                    self.log_progress(f"Processing file {obj['Key']}", i+1, total_files)
                    
                    # Download and parse JSON file
                    local_file = f"/tmp/{Path(obj['Key']).name}"
                    self.s3.download_file(self.bucket, obj['Key'], local_file)
                    
                    # Read JSON file - single object, normalize to DataFrame
                    with open(local_file, 'r') as f:
                        json_data = json.load(f)
                    df = pd.json_normalize([json_data])
                    dataframes.append(df)
                    
                    self.log_progress(f"Successfully loaded {len(df)} records from {obj['Key']}")
                    
                    # Clean up
                    os.remove(local_file)
                    
                except Exception as e:
                    self.log_error(f"Error processing {obj['Key']}", e)
                    continue
            
            if dataframes:
                combined_df = pd.concat(dataframes, ignore_index=True)
                self.log_progress(f"Combined data loading complete: {len(combined_df)} total records from {len(dataframes)} files")
                return combined_df
            else:
                self.log_error(f"No valid data loaded from {prefix}")
                return pd.DataFrame()
                
        except Exception as e:
            self.log_error(f"Failed to download raw data", e)
            return pd.DataFrame()
    
    def save_unified_output(self, 
                          df: pd.DataFrame, 
                          data_type: str = 'results',
                          script_name: str = 'processed') -> List[str]:
        """
        Save DataFrame in unified structure with 100-row batches and detailed progress tracking.
        
        Args:
            df: DataFrame to save
            data_type: 'results' or 'users'
            script_name: Current preprocessing script name
            
        Returns:
            List of S3 keys where files were saved
        """
        try:
            if df.empty:
                self.log_error("No data to save - DataFrame is empty")
                return []
            
            self.log_progress(f"Starting save of {len(df)} rows to {data_type} in {self.batch_size}-row batches")
            
            # Determine output path
            if data_type == 'users':
                s3_path = self.silver_users_path
            else:
                s3_path = self.silver_results_path
            
            saved_keys = []
            
            # Split into batches of 100 rows
            total_batches = (len(df) + self.batch_size - 1) // self.batch_size
            self.log_progress(f"Creating {total_batches} batches for upload")
            
            for batch_idx in range(total_batches):
                try:
                    start_idx = batch_idx * self.batch_size
                    end_idx = min(start_idx + self.batch_size, len(df))
                    batch_df = df.iloc[start_idx:end_idx]
                    
                    # Generate filename
                    filename = f"{script_name}_batch_{batch_idx:04d}.{self.output_format}"
                    s3_key = f"silver/{data_type}/{filename}"
                    
                    self.log_progress(f"Processing batch {batch_idx + 1}/{total_batches}: rows {start_idx}-{end_idx}")
                    
                    # Save to local temp file first
                    local_file = f"/tmp/{filename}"
                    
                    if self.output_format == 'parquet':
                        batch_df.to_parquet(local_file, index=False)
                    else:
                        batch_df.to_csv(local_file, index=False)
                    
                    # Upload to S3
                    self.s3.upload_file(local_file, self.bucket, s3_key)
                    self.log_progress(f"Successfully uploaded batch {batch_idx + 1}/{total_batches}: {s3_key}")
                    saved_keys.append(s3_key)
                    
                    # Clean up
                    os.remove(local_file)
                    
                except Exception as e:
                    self.log_error(f"Error processing batch {batch_idx + 1}/{total_batches}", e)
                    continue
            
            self.log_progress(f"Save complete: {len(saved_keys)}/{total_batches} batches successfully saved to {s3_path}")
            return saved_keys
            
        except Exception as e:
            self.log_error(f"Failed to save unified output", e)
            return []
    
    def load_previous_output(self, script_name: str, data_type: str = 'results') -> pd.DataFrame:
        """
        Load output from previous preprocessing step with detailed progress tracking.
        
        Args:
            script_name: Name of the previous script whose output to load
            data_type: 'results' or 'users'
            
        Returns:
            Combined DataFrame from all batch files
        """
        try:
            self.log_progress(f"Loading previous output from {script_name}...")
            
            prefix = f"silver/{data_type}/"
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            
            if 'Contents' not in response:
                self.log_error(f"No files found in {prefix}")
                return pd.DataFrame()
            
            # Find matching files
            matching_files = [obj for obj in response['Contents'] 
                            if (obj['Key'].endswith(f'.{self.output_format}') and 
                                script_name in obj['Key'])]
            
            if not matching_files:
                self.log_error(f"No previous output files found for {script_name} in {prefix}")
                return pd.DataFrame()
            
            self.log_progress(f"Found {len(matching_files)} output files from {script_name}")
            
            dataframes = []
            for i, obj in enumerate(matching_files):
                try:
                    self.log_progress(f"Loading file {obj['Key']}", i+1, len(matching_files))
                    
                    local_file = f"/tmp/{Path(obj['Key']).name}"
                    self.s3.download_file(self.bucket, obj['Key'], local_file)
                    
                    if self.output_format == 'parquet':
                        df = pd.read_parquet(local_file)
                    else:
                        df = pd.read_csv(local_file)
                    
                    dataframes.append(df)
                    self.log_progress(f"Successfully loaded {len(df)} records from {obj['Key']}")
                    
                    os.remove(local_file)
                    
                except Exception as e:
                    self.log_error(f"Error loading {obj['Key']}", e)
                    continue
            
            if dataframes:
                combined_df = pd.concat(dataframes, ignore_index=True)
                self.log_progress(f"Previous output loading complete: {len(combined_df)} total records from {len(dataframes)} files")
                return combined_df
            else:
                self.log_error(f"No valid data loaded from {script_name} output files")
                return pd.DataFrame()
                
        except Exception as e:
            self.log_error(f"Failed to load previous output from {script_name}", e)
            return pd.DataFrame()
    
    def get_processing_order(self) -> List[str]:
        """Return the correct preprocessing script execution order."""
        return [
            'preprocess_screensize_attention',
            'preprocess_render_delay',
            'preprocess_reaction_time',
            'preprocess_structure',
            'preprocess_create_boundingbox'
        ]
    
    def get_previous_script(self, current_script: str) -> Optional[str]:
        """Get the name of the previous script in the processing order."""
        order = self.get_processing_order()
        current_name = current_script.replace('.py', '')
        
        if current_name in order:
            idx = order.index(current_name)
            if idx > 0:
                return order[idx - 1]
        
        return None
    
    def upload_progress_log(self):
        """Upload progress log to S3 for external monitoring"""
        try:
            if os.path.exists(self.progress_file):
                s3_key = f"logs/{self.current_script}_progress.log"
                self.s3.upload_file(self.progress_file, self.bucket, s3_key)
                self.logger.info(f"Progress log uploaded to s3://{self.bucket}/{s3_key}")
        except Exception as e:
            self.log_error("Failed to upload progress log to S3", e)


# Helper function for easy import
def get_preprocessing_utils() -> PreprocessingUtils:
    """Factory function to create PreprocessingUtils instance."""
    return PreprocessingUtils()