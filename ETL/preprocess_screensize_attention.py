'''
This code is a comprehensive gaze data preprocessing pipeline that takes raw eye-tracking 
survey data from participants who viewed videos on different screen sizes and normalizes 
all gaze coordinates to a common reference frame. It starts by loading the raw CSV data 
and parsing each record to extract viewport dimensions (screen width/height) and gaze 
coordinate strings, then calculates exactly where the 1280x960 video appears within each 
participant's viewport by determining letterboxing offsets and scaling factors. 
The code determines a target viewport size (either the most common size if it covers >15% 
of participants, or the median dimensions), then transforms every gaze coordinate through 
a two-step process: coordinates that fall within the video display area are first converted 
to relative positions (0-1 range) within the video content, then scaled to the target viewport's 
video area to preserve their position relative to the actual video content, while coordinates 
outside the video area (like UI elements or letterbox regions) are simply scaled proportionally 
to fit the new viewport dimensions. Additionally, it extracts performance metrics from form 
data including hazard detection status, confidence levels, spacebar press timestamps, and 
session duration, applies conservative quality filtering that only removes records with 
severe calibration failures (less than 25% gaze points in valid areas), and outputs a processed 
dataset where all gaze coordinates are normalized to the same coordinate system, making them 
directly comparable across participants regardless of their original screen size, along 
with metadata about the video positioning, coordinate types, and processing statistics.
'''

import pandas as pd
import numpy as np
import ast
import re
import json
import os
from typing import Tuple, Dict, List
from dataclasses import dataclass

@dataclass
class VideoDisplayArea:
    """represents where the video appears within a viewport"""
    video_width: int
    video_height: int
    offset_x: int
    offset_y: int
    scale_factor: float

class ConservativeGazePreprocessor:
    def __init__(self, video_aspect_ratio: float = 1280/960, quality_threshold: float = 0.25):
        """
        initialize with conservative filtering approach
        quality_threshold: minimum retention rate (default 25% for calibration failure removal only)
        """
        self.video_aspect_ratio = video_aspect_ratio
        self.quality_threshold = quality_threshold
        self.target_viewport_size = None
        self.processing_stats = {}
        
    def calculate_video_display_area(self, viewport_width: int, viewport_height: int) -> VideoDisplayArea:
        """
        calculate exactly where the video appears within a given viewport
        """
        viewport_aspect = viewport_width / viewport_height
        
        if viewport_aspect > self.video_aspect_ratio:
            # viewport is wider than video - vertical letterboxing
            video_height = viewport_height
            video_width = int(video_height * self.video_aspect_ratio)
            offset_x = (viewport_width - video_width) // 2
            offset_y = 0
        else:
            # viewport is taller than video - horizontal letterboxing
            video_width = viewport_width
            video_height = int(video_width / self.video_aspect_ratio)
            offset_x = 0
            offset_y = (viewport_height - video_height) // 2
        
        scale_factor = video_width / 1280
        
        return VideoDisplayArea(
            video_width=video_width,
            video_height=video_height,
            offset_x=offset_x,
            offset_y=offset_y,
            scale_factor=scale_factor
        )
    
    def extract_viewport_dimensions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        extract viewport dimensions and parse gaze data
        """
        records = []
        parsing_errors = []
        
        for idx, row in df.iterrows():
            try:
                # extract viewport dimensions from json_normalize columns
                if 'windowDimensions.width' in row and 'windowDimensions.height' in row:
                    viewport_width = int(row['windowDimensions.width'])
                    viewport_height = int(row['windowDimensions.height'])
                elif 'windowDimensions' in row:
                    # fallback to old parsing method if nested object
                    window_dim_str = str(row['windowDimensions'])
                    width_match = re.search(r"'width':\s*(\d+)", window_dim_str)
                    height_match = re.search(r"'height':\s*(\d+)", window_dim_str)
                    
                    if not (width_match and height_match):
                        parsing_errors.append(f"row {idx}: could not parse viewport dimensions")
                        continue
                    
                    viewport_width = int(width_match.group(1))
                    viewport_height = int(height_match.group(1))
                else:
                    parsing_errors.append(f"row {idx}: windowDimensions not found")
                    continue
                
                # parse gaze data - handle both string and list formats
                if isinstance(row['gaze'], list):
                    # Already parsed by json_normalize
                    gaze_coords = row['gaze']
                else:
                    # String format, use existing parser
                    gaze_coords = self._parse_gaze_data(row['gaze'])
                
                if not gaze_coords:
                    parsing_errors.append(f"row {idx}: no valid gaze coordinates")
                    continue
                
                records.append({
                    'record_id': row['_id'],
                    'user_id': row['userId'],
                    'video_id': row['videoId'],
                    'viewport_width': viewport_width,
                    'viewport_height': viewport_height,
                    'viewport_aspect_ratio': round(viewport_width/viewport_height, 3),
                    'raw_gaze_coords': gaze_coords,
                    'form_data': {
                        col.replace('formData.', ''): row[col] 
                        for col in row.index if col.startswith('formData.')
                    }
                })
                
            except Exception as e:
                parsing_errors.append(f"row {idx}: {str(e)}")
                continue
        
        if parsing_errors:
            print(f"parsing errors ({len(parsing_errors)}):")
            for error in parsing_errors[:3]:
                print(f"  {error}")
            if len(parsing_errors) > 3:
                print(f"  ... and {len(parsing_errors) - 3} more")
        
        return pd.DataFrame(records)
    
    def _parse_gaze_data(self, gaze_str: str) -> List[Dict]:
        """
        parse gaze data string into list of coordinates
        """
        try:
            # clean and use ast.literal_eval
            cleaned_str = re.sub(r"ObjectId\([^)]+\)", '""', str(gaze_str))
            gaze_list = ast.literal_eval(cleaned_str)
            return gaze_list
        except:
            try:
                # regex extraction fallback
                coords = []
                x_matches = re.findall(r"'x':\s*([\d.]+)", str(gaze_str))
                y_matches = re.findall(r"'y':\s*([\d.]+)", str(gaze_str))
                time_matches = re.findall(r"'time':\s*([\d.]+)", str(gaze_str))
                
                for i in range(min(len(x_matches), len(y_matches), len(time_matches))):
                    coords.append({
                        'x': float(x_matches[i]),
                        'y': float(y_matches[i]),
                        'time': float(time_matches[i])
                    })
                return coords
            except:
                return []
    
    def process_gaze_coordinates(self, 
                               gaze_coords: List[Dict], 
                               viewport_width: int, 
                               viewport_height: int,
                               target_viewport: Tuple[int, int]) -> Dict:
        """
        process and transform gaze coordinates with conservative filtering
        """
        # calculate video display areas
        source_video_area = self.calculate_video_display_area(viewport_width, viewport_height)
        target_video_area = self.calculate_video_display_area(target_viewport[0], target_viewport[1])
        
        # separate video and non-video coordinates
        video_coords = []
        non_video_coords = []
        all_transformed_coords = []
        
        for coord in gaze_coords:
            try:
                x = float(coord['x'])
                y = float(coord['y'])
                time = coord.get('time', 0)
                
                # check if coordinate is within video display area
                within_video = (
                    source_video_area.offset_x <= x <= source_video_area.offset_x + source_video_area.video_width and
                    source_video_area.offset_y <= y <= source_video_area.offset_y + source_video_area.video_height
                )
                
                if within_video:
                    # convert to video-relative coordinates (0-1 range)
                    video_rel_x = (x - source_video_area.offset_x) / source_video_area.video_width
                    video_rel_y = (y - source_video_area.offset_y) / source_video_area.video_height
                    
                    # clamp to valid range
                    video_rel_x = max(0, min(1, video_rel_x))
                    video_rel_y = max(0, min(1, video_rel_y))
                    
                    # transform to target viewport coordinates
                    target_x = video_rel_x * target_video_area.video_width + target_video_area.offset_x
                    target_y = video_rel_y * target_video_area.video_height + target_video_area.offset_y
                    
                    video_coords.append({
                        'x': round(target_x, 2),
                        'y': round(target_y, 2),
                        'time': time,
                        'video_rel_x': round(video_rel_x, 4),
                        'video_rel_y': round(video_rel_y, 4),
                        'coordinate_type': 'video_area'
                    })
                    
                    all_transformed_coords.append({
                        'x': round(target_x, 2),
                        'y': round(target_y, 2),
                        'time': time,
                        'video_rel_x': round(video_rel_x, 4),
                        'video_rel_y': round(video_rel_y, 4),
                        'coordinate_type': 'video_area'
                    })
                else:
                    # keep non-video coordinates but mark them
                    # scale to target viewport proportionally
                    scale_x = target_viewport[0] / viewport_width
                    scale_y = target_viewport[1] / viewport_height
                    
                    scaled_x = x * scale_x
                    scaled_y = y * scale_y
                    
                    non_video_coord = {
                        'x': round(scaled_x, 2),
                        'y': round(scaled_y, 2),
                        'time': time,
                        'video_rel_x': None,
                        'video_rel_y': None,
                        'coordinate_type': 'non_video_area'
                    }
                    
                    non_video_coords.append(non_video_coord)
                    all_transformed_coords.append(non_video_coord)
                    
            except (KeyError, ValueError, TypeError):
                continue
        
        # calculate retention rate
        total_coords = len(gaze_coords)
        video_coords_count = len(video_coords)
        retention_rate = video_coords_count / total_coords if total_coords > 0 else 0
        
        return {
            'video_coords': video_coords,
            'non_video_coords': non_video_coords,
            'all_transformed_coords': all_transformed_coords,
            'total_points': total_coords,
            'video_points': video_coords_count,
            'non_video_points': len(non_video_coords),
            'retention_rate': retention_rate,
            'source_video_area': source_video_area,
            'target_video_area': target_video_area
        }
    
    def determine_target_viewport(self, records_df: pd.DataFrame, method: str = 'adaptive') -> Tuple[int, int]:
        """
        determine target viewport size
        """
        viewport_counts = records_df.groupby(['viewport_width', 'viewport_height']).size().sort_values(ascending=False)
        
        if method == 'adaptive':
            max_coverage = viewport_counts.iloc[0] / len(records_df) * 100
            if max_coverage >= 15:
                target_dims = viewport_counts.index[0]
                print(f"target viewport: {target_dims[0]}x{target_dims[1]} (most common, {max_coverage:.1f}% coverage)")
            else:
                median_width = int(records_df['viewport_width'].median())
                median_height = int(records_df['viewport_height'].median())
                target_dims = (median_width, median_height)
                print(f"target viewport: {target_dims[0]}x{target_dims[1]} (adaptive - using median)")
        
        self.target_viewport_size = target_dims
        return target_dims
    
    def extract_form_data_metrics(self, form_data_str: str) -> Dict:
        """
        extract key metrics from form data for quality assessment
        """
        try:
            cleaned_str = re.sub(r"ObjectId\([^)]+\)", '""', str(form_data_str))
            form_data = ast.literal_eval(cleaned_str)
            
            # extract performance indicators
            hazard_detected = form_data.get('hazardDetected', False)
            if hazard_detected in ['True', True, 'yes']:
                hazard_detected = True
            else:
                hazard_detected = False
                
            detection_confidence = int(form_data.get('detectionConfidence', 0))
            hazard_severity = int(form_data.get('hazardSeverity', 0))
            
            spacebar_timestamps = form_data.get('spacebarTimestamps', [])
            num_spacebar_presses = len(spacebar_timestamps) if spacebar_timestamps else 0
            
            start_time = form_data.get('startTime', 0)
            end_time = form_data.get('endTime', 0)
            session_duration = (end_time - start_time) / 1000 if end_time > start_time else 0
            
            return {
                'hazard_detected': hazard_detected,
                'detection_confidence': detection_confidence,
                'hazard_severity': hazard_severity,
                'num_spacebar_presses': num_spacebar_presses,
                'session_duration': session_duration,
                'spacebar_timestamps': spacebar_timestamps,
                'start_time': start_time,
                'end_time': end_time
            }
            
        except Exception as e:
            return {
                'hazard_detected': False,
                'detection_confidence': 0,
                'hazard_severity': 0,
                'num_spacebar_presses': 0,
                'session_duration': 0,
                'spacebar_timestamps': [],
                'start_time': 0,
                'end_time': 0
            }
    
    def process_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        main processing function with conservative filtering
        """
        print("starting conservative gaze preprocessing...")
        print(f"video aspect ratio: {self.video_aspect_ratio:.3f}")
        print(f"quality threshold: {self.quality_threshold:.1%} (calibration failure removal only)")
        
        # Debug: print available columns
        print(f"Total columns: {len(df.columns)}")
        print(f"Available columns: {list(df.columns)[:15]}")  # First 15 columns
        print(f"Has windowDimensions.width: {'windowDimensions.width' in df.columns}")
        print(f"Has gaze column: {'gaze' in df.columns}")
        if 'gaze' in df.columns:
            print(f"Gaze column type: {type(df['gaze'].iloc[0])}")
            print(f"Sample gaze data: {df['gaze'].iloc[0][:2] if isinstance(df['gaze'].iloc[0], list) else str(df['gaze'].iloc[0])[:100]}")  # First 2 items or first 100 chars
        
        # extract and parse data
        records_df = self.extract_viewport_dimensions(df)
        print(f"successfully parsed {len(records_df)} records")
        
        if len(records_df) == 0:
            raise ValueError("no valid records found")
        
        # determine target viewport
        target_viewport = self.determine_target_viewport(records_df)
        
        # process each record
        processed_records = []
        filtered_out_records = []
        total_original_points = 0
        total_video_points = 0
        
        for idx, row in records_df.iterrows():
            try:
                # process gaze coordinates
                processing_result = self.process_gaze_coordinates(
                    row['raw_gaze_coords'],
                    row['viewport_width'],
                    row['viewport_height'],
                    target_viewport
                )
                
                # extract form data metrics
                form_metrics = self.extract_form_data_metrics(row['form_data'])
                
                # conservative quality check - only remove clear calibration failures
                retention_rate = processing_result['retention_rate']
                if retention_rate < self.quality_threshold:
                    filtered_out_records.append({
                        'record_id': row['record_id'],
                        'user_id': row['user_id'],
                        'video_id': row['video_id'],
                        'retention_rate': retention_rate,
                        'filter_reason': 'calibration_failure'
                    })
                    continue
                
                total_original_points += processing_result['total_points']
                total_video_points += processing_result['video_points']
                
                # create final record
                processed_record = {
                    'record_id': row['record_id'],
                    'user_id': row['user_id'],
                    'video_id': row['video_id'],
                    
                    # viewport information
                    'original_viewport_width': row['viewport_width'],
                    'original_viewport_height': row['viewport_height'],
                    'original_viewport_aspect': row['viewport_aspect_ratio'],
                    'target_viewport_width': target_viewport[0],
                    'target_viewport_height': target_viewport[1],
                    'target_viewport_aspect': round(target_viewport[0]/target_viewport[1], 3),
                    
                    # video positioning
                    'original_video_width': processing_result['source_video_area'].video_width,
                    'original_video_height': processing_result['source_video_area'].video_height,
                    'original_video_offset_x': processing_result['source_video_area'].offset_x,
                    'original_video_offset_y': processing_result['source_video_area'].offset_y,
                    'target_video_width': processing_result['target_video_area'].video_width,
                    'target_video_height': processing_result['target_video_area'].video_height,
                    'target_video_offset_x': processing_result['target_video_area'].offset_x,
                    'target_video_offset_y': processing_result['target_video_area'].offset_y,
                    
                    # gaze statistics
                    'total_gaze_points': processing_result['total_points'],
                    'video_gaze_points': processing_result['video_points'],
                    'non_video_gaze_points': processing_result['non_video_points'],
                    'retention_rate': retention_rate,
                    
                    # performance metrics
                    'hazard_detected': form_metrics['hazard_detected'],
                    'detection_confidence': form_metrics['detection_confidence'],
                    'hazard_severity': form_metrics['hazard_severity'],
                    'num_spacebar_presses': form_metrics['num_spacebar_presses'],
                    'session_duration': form_metrics['session_duration'],
                    
                    # coordinate data (json strings for csv compatibility)
                    'all_transformed_coords': json.dumps(processing_result['all_transformed_coords']),
                    'video_coords_only': json.dumps(processing_result['video_coords']),
                    'spacebar_timestamps': json.dumps(form_metrics['spacebar_timestamps'])
                }
                
                processed_records.append(processed_record)
                
            except Exception as e:
                print(f"error processing record {idx}: {e}")
                continue
        
        processed_df = pd.DataFrame(processed_records)
        
        # store processing statistics
        self.processing_stats = {
            'total_input_records': len(df),
            'parsed_records': len(records_df),
            'processed_records': len(processed_df),
            'filtered_out_records': len(filtered_out_records),
            'total_original_gaze_points': total_original_points,
            'total_video_gaze_points': total_video_points,
            'overall_retention_rate': total_video_points / total_original_points if total_original_points > 0 else 0,
            'quality_threshold_applied': self.quality_threshold,
            'target_viewport': target_viewport,
            'filtering_approach': 'conservative'
        }
        
        print(f"\nprocessing complete:")
        print(f"  total input records: {len(df)}")
        print(f"  successfully parsed: {len(records_df)}")
        print(f"  passed quality threshold ({self.quality_threshold:.1%}): {len(processed_df)}")
        print(f"  filtered out (calibration failures): {len(filtered_out_records)}")
        print(f"  retention rate: {len(processed_df)/len(records_df):.1%}")
        print(f"  overall gaze retention: {self.processing_stats['overall_retention_rate']:.1%}")
        
        return processed_df

def main():
    """
    run the conservative preprocessing pipeline with unified S3 output and detailed logging
    """
    from preprocessing_utils import get_preprocessing_utils
    utils = get_preprocessing_utils()
    
    try:
        utils.log_progress("="*80)
        utils.log_progress("SCREENSIZE ATTENTION PREPROCESSING - Starting")
        utils.log_progress("="*80)
        
        # load raw data from S3
        utils.log_progress("Step 1: Loading raw survey data from S3")
        raw_df = utils.download_raw_data('results')
        if raw_df.empty:
            utils.log_error("No raw data found in S3 - cannot proceed")
            return pd.DataFrame()
        
        utils.log_progress(f"Step 1 Complete: Loaded {len(raw_df)} records from S3")
        
        # initialize preprocessor with conservative threshold
        utils.log_progress("Step 2: Initializing gaze preprocessor")
        preprocessor = ConservativeGazePreprocessor(
            video_aspect_ratio=1280/960,
            quality_threshold=0.25  # 25% threshold for calibration failures only
        )
        utils.log_progress("Step 2 Complete: Preprocessor initialized with 25% quality threshold")
        
        # process dataset
        utils.log_progress("Step 3: Starting gaze data processing (this may take several minutes)")
        processed_df = preprocessor.process_dataset(raw_df)
        
        if processed_df.empty:
            utils.log_error("Processing failed - no data returned from preprocessor")
            return pd.DataFrame()
        
        utils.log_progress(f"Step 3 Complete: Processed {len(processed_df)} records successfully")
        
        # save to unified S3 structure with 100-row batches
        utils.log_progress("Step 4: Saving processed data to S3 in parquet batches")
        saved_keys = utils.save_unified_output(
            processed_df, 
            data_type='results',
            script_name='screensize_attention'
        )
        
        if not saved_keys:
            utils.log_error("Failed to save output files to S3")
            return pd.DataFrame()
        
        # Upload progress log to S3
        utils.upload_progress_log()
        
        utils.log_progress("="*80)
        utils.log_progress("SCREENSIZE ATTENTION PREPROCESSING - COMPLETE!")
        utils.log_progress("="*80)
        utils.log_progress(f"SUCCESS: Saved {len(saved_keys)} batch files to S3")
        utils.log_progress(f"Processing Summary:")
        utils.log_progress(f"  Input records: {len(raw_df)}")
        utils.log_progress(f"  Output records: {len(processed_df)}")
        utils.log_progress(f"  Quality threshold: {preprocessor.quality_threshold:.1%}")
        utils.log_progress(f"  Target viewport: {preprocessor.target_viewport_size}")
        utils.log_progress(f"Ready for next step: render_delay preprocessing")
        
        return processed_df
        
    except Exception as e:
        utils.log_error("CRITICAL ERROR in screensize attention preprocessing", e)
        utils.upload_progress_log()  # Upload logs even on failure
        return pd.DataFrame()

if __name__ == "__main__":
    processed_data = main()