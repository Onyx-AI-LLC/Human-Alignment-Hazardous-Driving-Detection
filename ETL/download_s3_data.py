#!/usr/bin/env python3
"""
Download raw data from S3 to local data/ directory
Downloads results, users, and videos for local preprocessing
"""

import boto3
import os
import json
from pathlib import Path
from datetime import datetime

def setup_directories():
    """Create necessary local directories in project root"""
    # Get the project root directory (parent of etl directory)
    project_root = Path(__file__).parent.parent
    
    directories = [
        project_root / 'data' / 'raw',
        project_root / 'data' / 'processed', 
        project_root / 'data' / 'raw' / 'results',
        project_root / 'data' / 'raw' / 'users',
        project_root / 'data' / 'raw' / 'videos',
        project_root / 'models'
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"Success: Created directory: {directory}")

def download_s3_results(s3_client, bucket_name='hahd-primary-data-storage'):
    """Download survey results from S3"""
    print("\nStep 1: Downloading survey results from S3...")
    
    # Get the project root directory
    project_root = Path(__file__).parent.parent
    
    try:
        # List all files in raw/results/
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='raw/results/'
        )
        
        if 'Contents' not in response:
            print("No results data found in S3")
            return 0
        
        count = 0
        for obj in response['Contents']:
            if obj['Key'].endswith('.json'):
                # Download each result file to project root
                local_path = project_root / 'data' / obj['Key']
                s3_client.download_file(bucket_name, obj['Key'], str(local_path))
                count += 1
        
        print(f"Success: Downloaded {count} result files")
        return count
        
    except Exception as e:
        print(f"Error: Failed to download results: {e}")
        return 0

def download_s3_users(s3_client, bucket_name='hahd-primary-data-storage'):
    """Download user data from S3"""
    print("\nStep 2: Downloading user data from S3...")
    
    # Get the project root directory
    project_root = Path(__file__).parent.parent
    
    try:
        # List all files in raw/users/
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='raw/users/'
        )
        
        if 'Contents' not in response:
            print("No user data found in S3")
            return 0
        
        count = 0
        for obj in response['Contents']:
            if obj['Key'].endswith('.json'):
                # Download each user file to project root
                local_path = project_root / 'data' / obj['Key']
                s3_client.download_file(bucket_name, obj['Key'], str(local_path))
                count += 1
        
        print(f"Success: Downloaded {count} user files")
        return count
        
    except Exception as e:
        print(f"Error: Failed to download users: {e}")
        return 0

def download_s3_videos(s3_client, bucket_name='hahd-primary-data-storage', limit=None):
    """Download video files from S3"""
    print(f"\nStep 3: Downloading videos from S3 (limit={limit})...")
    
    # Get the project root directory
    project_root = Path(__file__).parent.parent
    
    try:
        # List all files in raw/videos/
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='raw/videos/',
            MaxKeys=limit if limit else 1000
        )
        
        if 'Contents' not in response:
            print("No video data found in S3")
            return 0
        
        count = 0
        total_size = 0
        
        for obj in response['Contents']:
            if obj['Key'].endswith(('.mp4', '.avi', '.mov')):
                # Download each video file to project root
                local_path = project_root / 'data' / obj['Key']
                file_size_mb = obj['Size'] / (1024 * 1024)
                
                print(f"Downloading {obj['Key']} ({file_size_mb:.1f} MB)...")
                s3_client.download_file(bucket_name, obj['Key'], str(local_path))
                
                count += 1
                total_size += obj['Size']
                
                if limit and count >= limit:
                    break
        
        print(f"Success: Downloaded {count} video files ({total_size/(1024*1024*1024):.2f} GB)")
        return count
        
    except Exception as e:
        print(f"Error: Failed to download videos: {e}")
        return 0

def create_local_csv_files(s3_client, bucket_name='hahd-primary-data-storage'):
    """Create consolidated CSV files from downloaded JSON data"""
    print("\nStep 4: Creating consolidated CSV files...")
    
    import pandas as pd
    
    # Get the project root directory
    project_root = Path(__file__).parent.parent
    
    try:
        # Process survey results
        results_data = []
        results_dir = project_root / 'data' / 'raw' / 'results'
        
        if results_dir.exists():
            for json_file in results_dir.glob('*.json'):
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    # Flatten the JSON structure
                    flattened = {
                        'user_id': data.get('userId', ''),
                        'video_id': data.get('videoId', ''),
                        'gaze_data': json.dumps(data.get('gaze', [])),
                        'hazard_detected': data.get('hazardDetected', False),
                        'reaction_time': data.get('reactionTime', 0),
                        'confidence_level': data.get('confidenceLevel', 0),
                        'completion_time': data.get('completionTime', 0),
                        'screen_width': data.get('screenWidth', 0),
                        'screen_height': data.get('screenHeight', 0),
                        'timestamp': data.get('timestamp', ''),
                        'spacebar_timestamps': json.dumps(data.get('spacebarTimestamps', [])),
                        'form_responses': json.dumps(data.get('formResponses', {}))
                    }
                    results_data.append(flattened)
        
        if results_data:
            df_results = pd.DataFrame(results_data)
            csv_path = project_root / 'data' / 'raw' / 'survey_results_raw.csv'
            df_results.to_csv(csv_path, index=False)
            print(f"Success: Created survey_results_raw.csv with {len(results_data)} records")
        
        # Process user data
        users_data = []
        users_dir = project_root / 'data' / 'raw' / 'users'
        
        if users_dir.exists():
            for json_file in users_dir.glob('*.json'):
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    # Flatten the user structure
                    flattened = {
                        'user_id': data.get('_id', ''),
                        'email': data.get('email', ''),
                        'referral_code': data.get('referralCode', ''),
                        'num_surveys_filled': data.get('numSurveysFilled', 0),
                        'country': data.get('country', ''),
                        'state': data.get('state', ''),
                        'city': data.get('city', ''),
                        'license_age': data.get('licenseAge', 0),
                        'age': data.get('age', 0),
                        'ethnicity': data.get('ethnicity', ''),
                        'gender': data.get('gender', ''),
                        'visually_impaired': data.get('visuallyImpaired', False),
                        'created_at': data.get('createdAt', '')
                    }
                    users_data.append(flattened)
        
        if users_data:
            df_users = pd.DataFrame(users_data)
            csv_path = project_root / 'data' / 'raw' / 'users_data_raw.csv'
            df_users.to_csv(csv_path, index=False)
            print(f"Success: Created users_data_raw.csv with {len(users_data)} records")
        
        return len(results_data), len(users_data)
        
    except Exception as e:
        print(f"Error: Failed to create CSV files: {e}")
        return 0, 0

def main():
    """Main download function"""
    print("Starting: S3 data download...")
    
    # Setup local directories
    setup_directories()
    
    # Initialize S3 client
    try:
        s3_client = boto3.client('s3')
        print("Success: Connected to S3")
    except Exception as e:
        print(f"Error: Failed to connect to S3: {e}")
        return
    
    # Download data
    bucket_name = 'hahd-primary-data-storage'
    
    results_count = download_s3_results(s3_client, bucket_name)
    users_count = download_s3_users(s3_client, bucket_name)
    
    # Ask user about video download limit
    print(f"\nVideo download options:")
    print("1. Download all videos (may take hours and lots of disk space)")
    print("2. Download first 10 videos (for testing)")
    print("3. Skip video download")
    
    choice = input("Enter choice (1/2/3): ").strip()
    
    if choice == "1":
        videos_count = download_s3_videos(s3_client, bucket_name)
    elif choice == "2":
        videos_count = download_s3_videos(s3_client, bucket_name, limit=10)
    else:
        videos_count = 0
        print("Skipping video download")
    
    # Create consolidated CSV files
    csv_results, csv_users = create_local_csv_files(s3_client, bucket_name)
    
    # Summary
    print(f"\nComplete: Download finished!")
    print(f"Survey results: {results_count} JSON files → {csv_results} CSV records")
    print(f"User data: {users_count} JSON files → {csv_users} CSV records") 
    print(f"Videos: {videos_count} files")
    
    # Get project root for display
    project_root = Path(__file__).parent.parent
    print(f"\nData structure created in project root:")
    print(f"  {project_root}/data/raw/survey_results_raw.csv")
    print(f"  {project_root}/data/raw/users_data_raw.csv")
    print(f"  {project_root}/data/raw/videos/ ({videos_count} videos)")
    print(f"  {project_root}/models/ (ready for model outputs)")

if __name__ == "__main__":
    main()