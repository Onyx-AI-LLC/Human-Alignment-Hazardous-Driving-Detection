#!/usr/bin/env python3
"""
Gold Tier Data Generation: Merge Silver Tier Data with YOLOv8 Features
This script combines preprocessed survey data with YOLOv8 object detection results
"""

import pandas as pd
import numpy as np
import boto3
import json
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YOLOv8FeatureExtractor:
    """Extract and process YOLOv8 features for ML training"""
    
    def __init__(self, bucket_name: str):
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        
    def load_yolov8_results(self) -> Dict:
        """Load all YOLOv8 results from data/yolo_hazard_output/"""
        yolo_data = {}
        
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix='data/yolo_hazard_output/',
                MaxKeys=1000
            )
            
            if 'Contents' not in response:
                logger.warning("No YOLOv8 results found in data/yolo_hazard_output/")
                return {}
            
            for obj in response['Contents']:
                if obj['Key'].endswith('_yolov8.json'):
                    try:
                        # Get video name (e.g., video1_yolov8.json -> video1)
                        video_name = os.path.basename(obj['Key']).replace('_yolov8.json', '')
                        
                        # Download and parse JSON
                        response_obj = self.s3.get_object(Bucket=self.bucket, Key=obj['Key'])
                        yolo_result = json.loads(response_obj['Body'].read().decode('utf-8'))
                        
                        yolo_data[video_name] = yolo_result
                        logger.info(f"Loaded YOLOv8 data for {video_name}")
                        
                    except Exception as e:
                        logger.error(f"Error loading {obj['Key']}: {e}")
                        continue
            
            logger.info(f"Loaded YOLOv8 results for {len(yolo_data)} videos")
            return yolo_data
            
        except Exception as e:
            logger.error(f"Error loading YOLOv8 results: {e}")
            return {}
    
    def extract_temporal_features(self, detections: List[Dict], timestamp_ms: float) -> Dict:
        """Extract YOLOv8 features for a specific timestamp"""
        features = {
            # Object counts by class
            'yolo_total_objects': 0,
            'yolo_vehicles': 0,
            'yolo_persons': 0,
            'yolo_traffic_signs': 0,
            'yolo_other_objects': 0,
            
            # Spatial features
            'yolo_avg_object_size': 0,
            'yolo_max_object_size': 0,
            'yolo_objects_center_region': 0,
            'yolo_objects_peripheral': 0,
            
            # Attention-related features
            'yolo_highest_confidence': 0,
            'yolo_avg_confidence': 0,
            'yolo_objects_near_center': 0,
            
            # Movement/hazard indicators
            'yolo_large_objects': 0,
            'yolo_multiple_vehicles': 0,
            'yolo_pedestrians_present': 0
        }
        
        if not detections:
            return features
        
        # Convert timestamp to frame number (assuming 30 FPS)
        if pd.isna(timestamp_ms) or timestamp_ms == 0:
            return features
        frame_number = int(timestamp_ms / (1000/30))
        
        # Find detections closest to this frame
        relevant_detections = []
        for detection in detections:
            frame_diff = abs(detection['frame'] - frame_number)
            if frame_diff <= 5:  # Within 5 frames (~167ms at 30fps)
                detection['frame_diff'] = frame_diff
                relevant_detections.append(detection)
        
        if not relevant_detections:
            return features
        
        # Sort by frame difference (closest first)
        relevant_detections.sort(key=lambda x: x['frame_diff'])
        
        # Extract features from relevant detections
        confidences = []
        object_sizes = []
        center_objects = 0
        peripheral_objects = 0
        
        # Define dangerous/important object classes
        vehicle_classes = ['car', 'truck', 'bus', 'motorcycle', 'bicycle']
        person_classes = ['person']
        sign_classes = ['stop sign', 'traffic light']
        
        for det in relevant_detections:
            class_name = det['class_name'].lower()
            confidence = det['confidence']
            bbox = det['bbox']  # [x1, y1, x2, y2]
            
            confidences.append(confidence)
            
            # Calculate object size (normalized area)
            if bbox and len(bbox) >= 4:
                width = abs(bbox[2] - bbox[0])
                height = abs(bbox[3] - bbox[1])
                size = width * height
                object_sizes.append(size)
                
                # Calculate center position
                center_x = (bbox[0] + bbox[2]) / 2
                center_y = (bbox[1] + bbox[3]) / 2
                
                # Check if object is in center region (middle third of screen)
                if 0.33 <= center_x <= 0.67 and 0.33 <= center_y <= 0.67:
                    center_objects += 1
                else:
                    peripheral_objects += 1
                
                # Check if object is near screen center (for attention correlation)
                distance_from_center = np.sqrt((center_x - 0.5)**2 + (center_y - 0.5)**2)
                if distance_from_center < 0.2:
                    features['yolo_objects_near_center'] += 1
            
            # Count by object type
            if any(vc in class_name for vc in vehicle_classes):
                features['yolo_vehicles'] += 1
            elif any(pc in class_name for pc in person_classes):
                features['yolo_persons'] += 1
            elif any(sc in class_name for sc in sign_classes):
                features['yolo_traffic_signs'] += 1
            else:
                features['yolo_other_objects'] += 1
        
        # Calculate aggregate features
        features['yolo_total_objects'] = len(relevant_detections)
        
        if confidences:
            features['yolo_highest_confidence'] = max(confidences)
            features['yolo_avg_confidence'] = np.mean(confidences)
        
        if object_sizes:
            features['yolo_avg_object_size'] = np.mean(object_sizes)
            features['yolo_max_object_size'] = max(object_sizes)
            features['yolo_large_objects'] = sum(1 for size in object_sizes if size > 0.1)
        
        features['yolo_objects_center_region'] = center_objects
        features['yolo_objects_peripheral'] = peripheral_objects
        
        # Hazard indicators
        features['yolo_multiple_vehicles'] = 1 if features['yolo_vehicles'] > 1 else 0
        features['yolo_pedestrians_present'] = 1 if features['yolo_persons'] > 0 else 0
        
        return features
    
    def merge_silver_with_yolo(self, silver_df: pd.DataFrame, yolo_data: Dict) -> pd.DataFrame:
        """Merge processed tier data with YOLOv8 features"""
        logger.info(f"Merging {len(silver_df)} processed records with YOLOv8 data")
        
        # Initialize YOLOv8 feature columns
        yolo_feature_columns = [
            'yolo_total_objects', 'yolo_vehicles', 'yolo_persons', 'yolo_traffic_signs', 
            'yolo_other_objects', 'yolo_avg_object_size', 'yolo_max_object_size',
            'yolo_objects_center_region', 'yolo_objects_peripheral', 'yolo_highest_confidence',
            'yolo_avg_confidence', 'yolo_objects_near_center', 'yolo_large_objects',
            'yolo_multiple_vehicles', 'yolo_pedestrians_present'
        ]
        
        # Initialize all YOLOv8 columns with zeros
        for col in yolo_feature_columns:
            silver_df[col] = 0
        
        # Add YOLOv8 match indicator
        silver_df['has_yolo_data'] = False
        
        matched_count = 0
        
        # Process each row in silver data
        for idx, row in silver_df.iterrows():
            video_id = row.get('video_id', '')
            timestamp = row.get('timestamp', 0)
            
            # Try to find matching YOLOv8 data
            video_key = None
            for yolo_video_name in yolo_data.keys():
                if str(video_id) in yolo_video_name or yolo_video_name in str(video_id):
                    video_key = yolo_video_name
                    break
            
            if video_key and video_key in yolo_data:
                yolo_result = yolo_data[video_key]
                detections = yolo_result.get('detections', [])
                
                if detections:
                    # Extract YOLOv8 features for this timestamp
                    yolo_features = self.extract_temporal_features(detections, timestamp)
                    
                    # Update the dataframe
                    for feature_name, feature_value in yolo_features.items():
                        silver_df.at[idx, feature_name] = feature_value
                    
                    silver_df.at[idx, 'has_yolo_data'] = True
                    matched_count += 1
        
        logger.info(f"Successfully matched {matched_count}/{len(silver_df)} records with YOLOv8 data")
        
        return silver_df


def load_silver_data(bucket_name: str) -> pd.DataFrame:
    """Load all processed tier parquet files"""
    s3 = boto3.client('s3')
    dfs = []
    
    try:
        # Load data/processed/ data
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix='data/processed/',
            MaxKeys=2000
        )
        
        if 'Contents' not in response:
            logger.error("No processed tier data found")
            return pd.DataFrame()
        
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet'):
                try:
                    # Download parquet file temporarily
                    local_file = f"/tmp/{os.path.basename(obj['Key'])}"
                    s3.download_file(bucket_name, obj['Key'], local_file)
                    
                    # Read parquet
                    df = pd.read_parquet(local_file)
                    dfs.append(df)
                    
                    # Cleanup
                    os.remove(local_file)
                    
                except Exception as e:
                    logger.error(f"Error loading {obj['Key']}: {e}")
                    continue
        
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"Loaded {len(combined_df)} records from {len(dfs)} processed tier files")
            return combined_df
        else:
            logger.error("No valid processed tier data loaded")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Error loading processed data: {e}")
        return pd.DataFrame()


def save_gold_data(df: pd.DataFrame, bucket_name: str) -> List[str]:
    """Save merged data to processed tier in batches"""
    s3 = boto3.client('s3')
    saved_keys = []
    
    # Save in batches of 1000 rows for performance
    batch_size = 1000
    
    for batch_idx in range(0, len(df), batch_size):
        batch_df = df.iloc[batch_idx:batch_idx + batch_size].copy()
        
        # Create filename
        batch_num = batch_idx // batch_size
        filename = f"gold_training_data_batch_{batch_num:04d}.parquet"
        local_file = f"/tmp/{filename}"
        s3_key = f"data/processed/{filename}"
        
        try:
            # Save as parquet
            batch_df.to_parquet(local_file, index=False)
            
            # Upload to S3
            s3.upload_file(local_file, bucket_name, s3_key)
            saved_keys.append(s3_key)
            
            # Cleanup
            os.remove(local_file)
            
            logger.info(f"Saved batch {batch_num} ({len(batch_df)} records) to {s3_key}")
            
        except Exception as e:
            logger.error(f"Error saving batch {batch_num}: {e}")
            continue
    
    return saved_keys


def main():
    bucket_name = os.environ.get('BUCKET_NAME', 'hahd-primary-data-storage')
    
    logger.info("Starting: Starting Final Data Processing with YOLOv8 Integration")
    logger.info("=" * 60)
    
    # Step 1: Load silver tier data
    logger.info("Step 1: Loading processed tier data...")
    silver_df = load_silver_data(bucket_name)
    
    if silver_df.empty:
        logger.error("No processed tier data found. Cannot proceed.")
        return False
    
    # Step 2: Load YOLOv8 results
    logger.info("Step 2: Loading YOLOv8 results...")
    extractor = YOLOv8FeatureExtractor(bucket_name)
    yolo_data = extractor.load_yolov8_results()
    
    if not yolo_data:
        logger.warning("No YOLOv8 data found. Proceeding with processed data only.")
    
    # Step 3: Merge data
    logger.info("Step 3: Merging processed data with YOLOv8 features...")
    gold_df = extractor.merge_silver_with_yolo(silver_df, yolo_data)
    
    # Step 4: Save merged data 
    logger.info("Step 4: Saving merged data to processed tier...")
    saved_keys = save_gold_data(gold_df, bucket_name)
    
    if saved_keys:
        logger.info("Complete: YOLOv8 Data Integration Complete!")
        logger.info("=" * 60)
        logger.info(f"Generated {len(saved_keys)} files with {len(gold_df)} total records")
        logger.info(f"YOLOv8 match rate: {gold_df['has_yolo_data'].mean()*100:.1f}%")
        
        # Print feature summary
        yolo_cols = [col for col in gold_df.columns if col.startswith('yolo_')]
        if yolo_cols:
            logger.info(f"Added {len(yolo_cols)} YOLOv8 features")
            logger.info("Top YOLOv8 features by variance:")
            for col in yolo_cols[:5]:
                if gold_df[col].var() > 0:
                    logger.info(f"  {col}: mean={gold_df[col].mean():.2f}, std={gold_df[col].std():.2f}")
        
        return True
    else:
        logger.error("Failed to save gold tier data")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)