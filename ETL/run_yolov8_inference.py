#!/usr/bin/env python3
"""
YOLOv8 Inference Script for HAHD Videos
Runs YOLOv8 object detection on driving videos and saves results to S3
"""

import os
import json
import boto3
import logging
from pathlib import Path
import subprocess
import cv2
from ultralytics import YOLO

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YOLOv8VideoProcessor:
    """Process driving videos with YOLOv8 object detection"""
    
    def __init__(self, bucket_name):
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        self.model = None
        
    def initialize_yolo(self):
        """Initialize YOLOv8 model"""
        try:
            logger.info(" Loading YOLOv8 model...")
            self.model = YOLO('yolov8n.pt')  # nano model for speed
            logger.info("Success: YOLOv8 model loaded successfully")
            return True
        except Exception as e:
            logger.error(f"Error: Failed to load YOLOv8 model: {e}")
            return False
    
    def get_videos_to_process(self):
        """Get list of videos that need processing"""
        try:
            # List videos in data/raw/videos/
            paginator = self.s3.get_paginator('list_objects_v2')
            video_pages = paginator.paginate(
                Bucket=self.bucket,
                Prefix='data/raw/videos/'
            )
            
            videos = []
            for page in video_pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith('.mp4'):
                            video_name = Path(key).stem
                            videos.append((key, video_name))
            
            logger.info(f"Found {len(videos)} videos in data/raw/videos/")
            
            # Check which ones already have YOLOv8 results
            existing_results = set()
            result_pages = paginator.paginate(
                Bucket=self.bucket,
                Prefix='data/yolo_hazard_output/'
            )
            
            for page in result_pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith('_yolov8.json'):
                            video_name = Path(key).stem.replace('_yolov8', '')
                            existing_results.add(video_name)
            
            logger.info(f"Found {len(existing_results)} existing YOLOv8 results")
            
            # Filter to only unprocessed videos
            to_process = [(key, name) for key, name in videos if name not in existing_results]
            logger.info(f"Need to process {len(to_process)} new videos")
            
            return to_process
            
        except Exception as e:
            logger.error(f"Error getting videos to process: {e}")
            return []
    
    def process_video(self, video_key, video_name):
        """Process single video with YOLOv8"""
        local_video = f"/tmp/{video_name}.mp4"
        results_file = f"/tmp/{video_name}_yolov8.json"
        
        try:
            # Download video
            logger.info(f"Downloading {video_key}")
            self.s3.download_file(self.bucket, video_key, local_video)
            
            # Process with YOLOv8
            logger.info(f" Processing {video_name} with YOLOv8...")
            results = self.model(local_video)
            
            # Extract detections for each frame
            detections = []
            for r in results:
                frame_detections = []
                if r.boxes is not None:
                    for box in r.boxes:
                        detection = {
                            'bbox': box.xyxy[0].tolist(),  # [x1, y1, x2, y2]
                            'confidence': float(box.conf[0]),
                            'class': int(box.cls[0]),
                            'class_name': self.model.names[int(box.cls[0])]
                        }
                        frame_detections.append(detection)
                detections.append(frame_detections)
            
            # Save results
            result_data = {
                'video_name': video_name,
                'total_frames': len(detections),
                'detections': detections,
                'model_info': {
                    'model': 'yolov8n.pt',
                    'version': '8.0',
                    'classes': self.model.names
                }
            }
            
            with open(results_file, 'w') as f:
                json.dump(result_data, f)
            
            # Upload to S3
            s3_key = f"data/yolo_hazard_output/{video_name}_yolov8.json"
            self.s3.upload_file(results_file, self.bucket, s3_key)
            logger.info(f"Success: Uploaded results: {s3_key}")
            
            # Cleanup
            os.remove(local_video)
            os.remove(results_file)
            
            return True
            
        except Exception as e:
            logger.error(f"Error: Error processing {video_name}: {e}")
            # Cleanup on error
            for file in [local_video, results_file]:
                if os.path.exists(file):
                    os.remove(file)
            return False
    
    def run(self):
        """Run YOLOv8 processing pipeline"""
        logger.info("Starting: Starting YOLOv8 processing pipeline")
        logger.info("=" * 60)
        
        # Initialize YOLO
        if not self.initialize_yolo():
            return False
        
        # Get videos to process
        videos = self.get_videos_to_process()
        
        if not videos:
            logger.info("Success: No new videos to process")
            return True
        
        # Process each video
        processed = 0
        failed = 0
        
        for video_key, video_name in videos:
            if self.process_video(video_key, video_name):
                processed += 1
            else:
                failed += 1
                
            logger.info(f"Progress: {processed + failed}/{len(videos)} videos processed")
        
        logger.info("Complete: YOLOv8 processing completed!")
        logger.info("=" * 60)
        logger.info(f"Success: Successfully processed: {processed}")
        logger.info(f"Error: Failed: {failed}")
        logger.info(f" Total: {len(videos)}")
        
        return failed == 0


def main():
    bucket_name = os.environ.get('BUCKET_NAME', 'hahd-primary-data-storage')
    
    processor = YOLOv8VideoProcessor(bucket_name)
    success = processor.run()
    
    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)