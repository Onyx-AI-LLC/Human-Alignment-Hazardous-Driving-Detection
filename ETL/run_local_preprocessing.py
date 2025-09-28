#!/usr/bin/env python3
"""
Complete local HAHD preprocessing pipeline
Matches the notebook processing pipeline exactly for local execution
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Add etl directory to path for imports
sys.path.append('etl')

def setup_directories():
    """Create necessary processing directories"""
    directories = [
        'data/processed',
        'data/yolo_hazard_output',
        'data/yolo_hazard_output/frames',
        'models'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"Success: Created directory: {directory}")

def step1_conservative_gaze_preprocessing():
    """Step 1: Screen size and attention preprocessing (ConservativeGazePreprocessor)"""
    print("\nStep 1: Running Conservative Gaze Preprocessing...")
    
    try:
        # Import the preprocessing class from etl
        from etl.preprocess_screensize_attention import ConservativeGazePreprocessor
        
        # Initialize processor
        processor = ConservativeGazePreprocessor()
        
        # Set input/output paths (matching notebook)
        input_file = 'data/raw/survey_results_raw.csv'
        output_dir = 'data/processed'
        
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        # Process the data
        result = processor.main(input_file, output_dir)
        
        print(f"Success: Step 1 complete: {result}")
        return result
        
    except Exception as e:
        print(f"Error: Step 1 failed: {e}")
        raise

def step2_last15_seconds_extraction():
    """Step 2: Extract last 15 seconds to eliminate render delay"""
    print("\nStep 2: Running Last 15 Seconds Extraction...")
    
    try:
        from etl.preprocess_render_delay import Last15SecondsExtractor
        
        extractor = Last15SecondsExtractor()
        
        # Set paths
        input_csv = "data/processed/screen&gaze_scaled.csv"
        output_csv = "data/processed/screen&gaze_scaled_last15s.csv"
        
        if not os.path.exists(input_csv):
            raise FileNotFoundError(f"Input file not found: {input_csv}")
        
        result = extractor.save_processed_data(input_csv, output_csv)
        
        print(f"Success: Step 2 complete: {result}")
        return result
        
    except Exception as e:
        print(f"Error: Step 2 failed: {e}")
        raise

def step3_reaction_time_adjustment():
    """Step 3: Adjust for human reaction time based on demographics"""
    print("\nStep 3: Running Reaction Time Adjustment...")
    
    try:
        from etl.preprocess_reaction_time import ReactionTimeAdjuster
        
        adjuster = ReactionTimeAdjuster()
        
        # Set paths
        gaze_csv = "data/processed/screen&gaze_scaled_last15s.csv"
        users_csv = "data/raw/users_data_raw.csv"
        output_csv = "data/processed/screen&gaze_scaled_last15s_adjusted.csv"
        
        if not os.path.exists(gaze_csv):
            raise FileNotFoundError(f"Gaze file not found: {gaze_csv}")
        if not os.path.exists(users_csv):
            raise FileNotFoundError(f"Users file not found: {users_csv}")
        
        result = adjuster.save_adjusted_data(gaze_csv, users_csv, output_csv)
        
        print(f"Success: Step 3 complete: {result}")
        return result
        
    except Exception as e:
        print(f"Error: Step 3 failed: {e}")
        raise

def step4_hazard_dataset_creation():
    """Step 4: Create structured hazard training dataset"""
    print("\nStep 4: Running Hazard Dataset Creation...")
    
    try:
        from etl.preprocess_structure import HazardDatasetCreator
        
        creator = HazardDatasetCreator()
        
        # Set paths
        input_file = "data/processed/screen&gaze_scaled_last15s_adjusted.csv"
        output_file = "data/processed/hazard_training_dataset.csv"
        
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        result = creator.process_gaze_data(input_file, output_file)
        
        print(f"Success: Step 4 complete: {result}")
        return result
        
    except Exception as e:
        print(f"Error: Step 4 failed: {e}")
        raise

def step5_gaze_pattern_analysis():
    """Step 5: Analyze gaze patterns and prepare for YOLO integration"""
    print("\nStep 5: Running Gaze Pattern Analysis...")
    
    try:
        from etl.preprocess_create_boundingbox import GazePatternAnalyzer
        
        analyzer = GazePatternAnalyzer()
        
        # Set paths
        csv_path = "data/processed/hazard_training_dataset.csv"
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Input file not found: {csv_path}")
        
        result = analyzer.run_eda(csv_path)
        
        print(f"Success: Step 5 complete: {result}")
        return result
        
    except Exception as e:
        print(f"Error: Step 5 failed: {e}")
        raise

def step6_yolo_inference():
    """Step 6: Run YOLOv8 inference on videos"""
    print("\nStep 6: Running YOLOv8 Inference...")
    
    try:
        from etl.run_yolov8_inference import main as yolo_main
        
        # Check if videos exist
        videos_dir = Path('data/raw/videos')
        if not videos_dir.exists() or not list(videos_dir.glob('*.mp4')):
            print("Warning: No videos found - skipping YOLOv8 inference")
            print("Run download_s3_data.py first to get videos")
            return "Skipped - no videos"
        
        result = yolo_main()
        
        print(f"Success: Step 6 complete: {result}")
        return result
        
    except Exception as e:
        print(f"Error: Step 6 failed: {e}")
        raise

def step7_merge_yolo_with_gaze():
    """Step 7: Merge YOLOv8 detections with gaze data"""
    print("\nStep 7: Merging YOLOv8 with Gaze Data...")
    
    try:
        from etl.merge_silver_with_yolov8 import main as merge_main
        
        # Check if YOLO output exists
        yolo_output = Path('data/yolo_hazard_output')
        if not yolo_output.exists():
            print("Warning: No YOLOv8 output found - creating basic training dataset")
            # Copy hazard dataset as final output
            import shutil
            shutil.copy(
                'data/processed/hazard_training_dataset.csv',
                'data/processed/hazard_training_final.csv'
            )
            return "Basic dataset created (no YOLO features)"
        
        result = merge_main()
        
        print(f"Success: Step 7 complete: {result}")
        return result
        
    except Exception as e:
        print(f"Error: Step 7 failed: {e}")
        raise

def step8_train_hazard_model():
    """Step 8: Train the hazard detection model"""
    print("\nStep 8: Training Hazard Detection Model...")
    
    try:
        from etl.train_hazard_model import main as train_main
        
        # Check if final dataset exists
        final_dataset = 'data/processed/hazard_training_final.csv'
        if not os.path.exists(final_dataset):
            final_dataset = 'data/processed/hazard_training_dataset.csv'
        
        if not os.path.exists(final_dataset):
            raise FileNotFoundError(f"Training dataset not found")
        
        result = train_main()
        
        print(f"Success: Step 8 complete: {result}")
        return result
        
    except Exception as e:
        print(f"Error: Step 8 failed: {e}")
        raise

def main():
    """Run complete local preprocessing pipeline"""
    print("Starting: Complete Local HAHD Preprocessing Pipeline...")
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    # Setup directories
    setup_directories()
    
    # Check if raw data exists
    if not os.path.exists('data/raw/survey_results_raw.csv'):
        print("Error: Raw data not found!")
        print("Please run: python download_s3_data.py")
        return
    
    results = {}
    
    try:
        # Run all preprocessing steps
        results['step1'] = step1_conservative_gaze_preprocessing()
        results['step2'] = step2_last15_seconds_extraction()
        results['step3'] = step3_reaction_time_adjustment()
        results['step4'] = step4_hazard_dataset_creation()
        results['step5'] = step5_gaze_pattern_analysis()
        results['step6'] = step6_yolo_inference()
        results['step7'] = step7_merge_yolo_with_gaze()
        results['step8'] = step8_train_hazard_model()
        
        # Create processing summary
        summary = {
            'processing_timestamp': datetime.now().isoformat(),
            'processing_type': 'complete_local_preprocessing_pipeline',
            'pipeline_steps': [
                {'step': 1, 'name': 'Conservative Gaze Preprocessing', 'status': 'completed', 'result': str(results['step1'])},
                {'step': 2, 'name': 'Last 15 Seconds Extraction', 'status': 'completed', 'result': str(results['step2'])},
                {'step': 3, 'name': 'Reaction Time Adjustment', 'status': 'completed', 'result': str(results['step3'])},
                {'step': 4, 'name': 'Hazard Dataset Creation', 'status': 'completed', 'result': str(results['step4'])},
                {'step': 5, 'name': 'Gaze Pattern Analysis', 'status': 'completed', 'result': str(results['step5'])},
                {'step': 6, 'name': 'YOLOv8 Inference', 'status': 'completed', 'result': str(results['step6'])},
                {'step': 7, 'name': 'Merge YOLO with Gaze', 'status': 'completed', 'result': str(results['step7'])},
                {'step': 8, 'name': 'Train Hazard Model', 'status': 'completed', 'result': str(results['step8'])}
            ],
            'training_dataset_ready': True,
            'output_files': [
                'data/processed/hazard_training_final.csv',
                'data/yolo_hazard_output/',
                'models/hazard_detection_model.pkl'
            ]
        }
        
        # Save summary
        with open('data/processed/local_preprocessing_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        print("\nComplete: Local preprocessing pipeline finished!")
        print(f" Final training dataset: data/processed/hazard_training_final.csv")
        print(f" Trained model: models/hazard_detection_model.pkl")
        print(f" Summary: data/processed/local_preprocessing_summary.json")
        
        return summary
        
    except Exception as e:
        print(f"Error: Pipeline failed: {e}")
        error_summary = {
            'error_timestamp': datetime.now().isoformat(),
            'error_message': str(e),
            'pipeline_status': 'failed',
            'completed_steps': results
        }
        
        with open('data/processed/preprocessing_error.json', 'w') as f:
            json.dump(error_summary, f, indent=2)
        
        raise

if __name__ == "__main__":
    main()