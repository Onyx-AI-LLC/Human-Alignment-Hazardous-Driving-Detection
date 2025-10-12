import json
import boto3
import os
import pandas as pd
import io
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """Backfill preprocessing Lambda - process all raw data to silver with CSV/Parquet format"""
    
    bucket_name = os.environ.get('BUCKET_NAME', 'hahd-primary-data-storage')
    
    print("Starting: Starting backfill preprocessing for all raw data...")
    
    try:
        # Create silver directory structure
        create_silver_structure(bucket_name)
        
        # Process all raw directories and save as CSV/Parquet
        results = {}
        results['users'] = process_raw_users_to_csv(bucket_name)
        results['results'] = process_raw_results_to_csv(bucket_name)  
        results['videos'] = process_raw_videos_to_csv(bucket_name)
        
        # Create processing summary
        summary = {
            'processing_timestamp': datetime.now().isoformat(),
            'processing_type': 'backfill_csv_parquet',
            'processed_counts': results,
            'silver_structure_created': True,
            'output_formats': ['csv', 'parquet']
        }
        
        # Save processing summary
        s3.put_object(
            Bucket=bucket_name,
            Key='silver/processing_summary.json',
            Body=json.dumps(summary, indent=2),
            ContentType='application/json'
        )
        
        total_processed = sum(results.values())
        print(f"Success: Backfill complete! Processed {total_processed} total items as CSV/Parquet")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Backfill preprocessing completed with CSV/Parquet output',
                'processed_counts': results,
                'total_items': total_processed,
                'timestamp': datetime.now().isoformat()
            })
        }
        
    except Exception as e:
        print(f"Error: Error during backfill: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def create_silver_structure(bucket_name):
    """Create silver directory structure matching raw"""
    print(" Creating silver directory structure...")
    
    directories = ['silver/users/', 'silver/results/', 'silver/videos/']
    
    for directory in directories:
        try:
            s3.put_object(
                Bucket=bucket_name,
                Key=directory,
                Body='',
                ContentType='application/x-directory'
            )
            print(f"Created directory: {directory}")
        except Exception as e:
            print(f"Directory may already exist: {directory} - {e}")

def process_raw_users_to_csv(bucket_name):
    """Process all files in raw/users/ to silver/users/ as CSV/Parquet"""
    print(" Processing raw/users/ to CSV/Parquet...")
    
    try:
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix='raw/users/'
        )
        
        if 'Contents' not in response:
            print("No users data found")
            return 0
        
        # Collect all user data into a list for DataFrame
        all_users = []
        
        for obj in response['Contents']:
            if obj['Key'].endswith('.json'):
                # Download original file
                file_obj = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                user_data = json.loads(file_obj['Body'].read().decode('utf-8'))
                
                # Flatten user data for CSV using correct field names
                processed_user = {
                    'user_id': user_data.get('_id', 'unknown'),
                    'email': user_data.get('email', ''),
                    'referral_code': user_data.get('referralCode', ''),
                    'num_surveys_filled': user_data.get('numSurveysFilled', 0),
                    'country': user_data.get('country', ''),
                    'state': user_data.get('state', ''),
                    'city': user_data.get('city', ''),
                    'license_age': user_data.get('licenseAge', 0),
                    'age': user_data.get('age', 0),
                    'ethnicity': user_data.get('ethnicity', ''),
                    'gender': user_data.get('gender', ''),
                    'visually_impaired': user_data.get('visuallyImpaired', False),
                    'created_at': user_data.get('createdAt', ''),
                    'processed_timestamp': datetime.now().isoformat(),
                    'processing_stage': 'silver',
                    'original_file': obj['Key']
                }
                
                all_users.append(processed_user)
        
        if all_users:
            # Convert to DataFrame
            df_users = pd.DataFrame(all_users)
            
            # Save as CSV
            csv_buffer = io.StringIO()
            df_users.to_csv(csv_buffer, index=False)
            s3.put_object(
                Bucket=bucket_name,
                Key='silver/users/processed_users.csv',
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            
            # Save as Parquet
            parquet_buffer = io.BytesIO()
            df_users.to_parquet(parquet_buffer, index=False)
            s3.put_object(
                Bucket=bucket_name,
                Key='silver/users/processed_users.parquet',
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            print(f"Success: Processed {len(all_users)} user files to CSV/Parquet")
            return len(all_users)
        
        return 0
        
    except Exception as e:
        print(f"Error processing users: {e}")
        return 0

def process_raw_results_to_csv(bucket_name):
    """Process all files in raw/results/ to silver/results/ as CSV/Parquet"""
    print(" Processing raw/results/ to CSV/Parquet...")
    
    try:
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix='raw/results/'
        )
        
        if 'Contents' not in response:
            print("No results data found")
            return 0
        
        # Process in batches to avoid memory issues
        batch_size = 100
        all_results = []
        
        for obj in response['Contents']:
            if obj['Key'].endswith('.json'):
                # Download original file
                file_obj = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                result_data = json.loads(file_obj['Body'].read().decode('utf-8'))
                
                # Flatten survey result data for CSV using correct field names
                processed_result = {
                    'user_id': result_data.get('userId', 'unknown'),
                    'video_id': result_data.get('videoId', 'unknown'),
                    'gaze_data_count': len(result_data.get('gaze', [])),
                    'hazard_detected': result_data.get('hazardDetected', False),
                    'reaction_time': result_data.get('reactionTime', 0),
                    'confidence_level': result_data.get('confidenceLevel', 0),
                    'completion_time': result_data.get('completionTime', 0),
                    'screen_width': result_data.get('screenWidth', 0),
                    'screen_height': result_data.get('screenHeight', 0),
                    'timestamp': result_data.get('timestamp', ''),
                    'processed_timestamp': datetime.now().isoformat(),
                    'processing_stage': 'silver',
                    'original_file': obj['Key']
                }
                
                all_results.append(processed_result)
        
        if all_results:
            # Convert to DataFrame
            df_results = pd.DataFrame(all_results)
            
            # Save as CSV
            csv_buffer = io.StringIO()
            df_results.to_csv(csv_buffer, index=False)
            s3.put_object(
                Bucket=bucket_name,
                Key='silver/results/processed_survey_results.csv',
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            
            # Save as Parquet
            parquet_buffer = io.BytesIO()
            df_results.to_parquet(parquet_buffer, index=False)
            s3.put_object(
                Bucket=bucket_name,
                Key='silver/results/processed_survey_results.parquet',
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            print(f"Success: Processed {len(all_results)} result files to CSV/Parquet")
            return len(all_results)
        
        return 0
        
    except Exception as e:
        print(f"Error processing results: {e}")
        return 0

def process_raw_videos_to_csv(bucket_name):
    """Process all files in raw/videos/ to silver/videos/ as CSV/Parquet"""
    print(" Processing raw/videos/ to CSV/Parquet...")
    
    try:
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix='raw/videos/'
        )
        
        if 'Contents' not in response:
            print("No video data found")
            return 0
        
        all_videos = []
        
        for obj in response['Contents']:
            # For videos, create metadata entries
            if obj['Key'].endswith(('.mp4', '.avi', '.mov')):
                video_filename = os.path.basename(obj['Key'])
                
                # Create video metadata
                video_metadata = {
                    'video_filename': video_filename,
                    'video_path': obj['Key'],
                    'file_size_bytes': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat() if 'LastModified' in obj else '',
                    'status': 'available_for_processing',
                    'processing_stage': 'silver',
                    'yolo_processed': False,
                    'created_timestamp': datetime.now().isoformat()
                }
                
                all_videos.append(video_metadata)
        
        if all_videos:
            # Convert to DataFrame
            df_videos = pd.DataFrame(all_videos)
            
            # Save as CSV
            csv_buffer = io.StringIO()
            df_videos.to_csv(csv_buffer, index=False)
            s3.put_object(
                Bucket=bucket_name,
                Key='silver/videos/video_metadata.csv',
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            
            # Save as Parquet
            parquet_buffer = io.BytesIO()
            df_videos.to_parquet(parquet_buffer, index=False)
            s3.put_object(
                Bucket=bucket_name,
                Key='silver/videos/video_metadata.parquet',
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            print(f"Success: Processed {len(all_videos)} video metadata files to CSV/Parquet")
            return len(all_videos)
        
        return 0
        
    except Exception as e:
        print(f"Error processing videos: {e}")
        return 0

def process_gaze_data_simple(gaze_data):
    """Simple gaze data processing"""
    if not gaze_data:
        return []
    
    try:
        # If it's already a list, return it
        if isinstance(gaze_data, list):
            return gaze_data
        elif isinstance(gaze_data, str):
            # Try to parse string as JSON/list
            import ast
            return ast.literal_eval(gaze_data)
        else:
            return []
    except:
        return []