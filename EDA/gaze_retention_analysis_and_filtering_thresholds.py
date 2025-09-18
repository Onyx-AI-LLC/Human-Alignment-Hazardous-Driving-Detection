"""
Analyzes gaze data retention rates and generates evidence-based filtering threshold recommendations.
Transforms gaze coordinates across different viewport sizes and correlates retention with task performance.
Outputs processed data, analysis results, and comprehensive visualizations to support threshold decisions.
"""

import pandas as pd
import numpy as np
import ast
import re
import json
from typing import Tuple, Dict, List, Optional
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from scipy.stats import pearsonr, spearmanr
from dataclasses import dataclass
import os

@dataclass
class VideoDisplayArea:
    """represents where the video appears within a viewport"""
    video_width: int
    video_height: int
    offset_x: int
    offset_y: int
    scale_factor: float

class FoolproofVideoPreprocessor:
    def __init__(self, video_aspect_ratio: float = 1280/960):
        """
        initialize with the actual video aspect ratio
        default is 1280/960 = 1.333 (4:3)
        """
        self.video_aspect_ratio = video_aspect_ratio
        self.target_viewport_size = None
        self.processing_stats = {}
        
    def calculate_video_display_area(self, viewport_width: int, viewport_height: int) -> VideoDisplayArea:
        """
        calculate exactly where the video appears within a given viewport
        accounts for letterboxing to maintain aspect ratio
        """
        viewport_aspect = viewport_width / viewport_height
        
        if viewport_aspect > self.video_aspect_ratio:
            # viewport is wider than video - vertical letterboxing (black bars on sides)
            # video height fills viewport height
            video_height = viewport_height
            video_width = int(video_height * self.video_aspect_ratio)
            offset_x = (viewport_width - video_width) // 2
            offset_y = 0
        else:
            # viewport is taller than video - horizontal letterboxing (black bars top/bottom)
            # video width fills viewport width
            video_width = viewport_width
            video_height = int(video_width / self.video_aspect_ratio)
            offset_x = 0
            offset_y = (viewport_height - video_height) // 2
        
        # scale factor represents how much larger the displayed video is vs original 1280x960
        scale_factor = video_width / 1280  # or video_height / 960, should be same
        
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
                # extract viewport dimensions
                window_dim_str = str(row['windowDimensions'])
                width_match = re.search(r"'width':\s*(\d+)", window_dim_str)
                height_match = re.search(r"'height':\s*(\d+)", window_dim_str)
                
                if not (width_match and height_match):
                    parsing_errors.append(f"row {idx}: could not parse viewport dimensions")
                    continue
                
                viewport_width = int(width_match.group(1))
                viewport_height = int(height_match.group(1))
                
                # parse gaze data
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
                    'form_data': row['formData']
                })
                
            except Exception as e:
                parsing_errors.append(f"row {idx}: {str(e)}")
                continue
        
        if parsing_errors:
            print(f"parsing errors ({len(parsing_errors)}):")
            for error in parsing_errors[:5]:  # show first 5
                print(f"  {error}")
            if len(parsing_errors) > 5:
                print(f"  ... and {len(parsing_errors) - 5} more")
        
        return pd.DataFrame(records)
    
    def _parse_gaze_data(self, gaze_str: str) -> List[Dict]:
        """
        parse gaze data string into list of coordinates
        """
        try:
            # method 1: clean and use ast.literal_eval
            cleaned_str = re.sub(r"ObjectId\([^)]+\)", '""', str(gaze_str))
            gaze_list = ast.literal_eval(cleaned_str)
            return gaze_list
        except:
            try:
                # method 2: regex extraction as fallback
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
    
    def filter_and_transform_gaze_coordinates(self, 
                                            gaze_coords: List[Dict], 
                                            viewport_width: int, 
                                            viewport_height: int,
                                            target_viewport: Tuple[int, int]) -> Dict:
        """
        filter gaze coordinates to video area and transform to target viewport
        returns detailed transformation results
        """
        # calculate video display areas
        source_video_area = self.calculate_video_display_area(viewport_width, viewport_height)
        target_video_area = self.calculate_video_display_area(target_viewport[0], target_viewport[1])
        
        valid_coords = []
        invalid_coords = []
        transformed_coords = []
        
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
                    
                    # clamp to valid range (floating point precision issues)
                    video_rel_x = max(0, min(1, video_rel_x))
                    video_rel_y = max(0, min(1, video_rel_y))
                    
                    # transform to target viewport coordinates
                    target_x = video_rel_x * target_video_area.video_width + target_video_area.offset_x
                    target_y = video_rel_y * target_video_area.video_height + target_video_area.offset_y
                    
                    valid_coords.append({
                        'original_x': x,
                        'original_y': y,
                        'video_rel_x': round(video_rel_x, 4),
                        'video_rel_y': round(video_rel_y, 4),
                        'target_x': round(target_x, 2),
                        'target_y': round(target_y, 2),
                        'time': time
                    })
                    
                    transformed_coords.append({
                        'x': round(target_x, 2),
                        'y': round(target_y, 2),
                        'time': time,
                        'video_rel_x': round(video_rel_x, 4),
                        'video_rel_y': round(video_rel_y, 4)
                    })
                else:
                    invalid_coords.append({
                        'x': x,
                        'y': y,
                        'time': time,
                        'reason': 'outside_video_area'
                    })
                    
            except (KeyError, ValueError, TypeError):
                invalid_coords.append({
                    'x': coord.get('x', 'unknown'),
                    'y': coord.get('y', 'unknown'),
                    'time': coord.get('time', 'unknown'),
                    'reason': 'parsing_error'
                })
        
        return {
            'transformed_coords': transformed_coords,
            'valid_coords': valid_coords,
            'invalid_coords': invalid_coords,
            'source_video_area': source_video_area,
            'target_video_area': target_video_area,
            'total_points': len(gaze_coords),
            'valid_points': len(valid_coords),
            'invalid_points': len(invalid_coords),
            'retention_rate': len(valid_coords) / len(gaze_coords) if gaze_coords else 0
        }
    
    def determine_target_viewport(self, records_df: pd.DataFrame, method: str = 'adaptive') -> Tuple[int, int]:
        """
        dynamically determine target viewport size
        """
        viewport_counts = records_df.groupby(['viewport_width', 'viewport_height']).size().sort_values(ascending=False)
        
        if method == 'most_common':
            target_dims = viewport_counts.index[0]
            coverage = viewport_counts.iloc[0] / len(records_df) * 100
            print(f"target viewport: {target_dims[0]}x{target_dims[1]} (covers {coverage:.1f}% of data)")
        
        elif method == 'median':
            median_width = int(records_df['viewport_width'].median())
            median_height = int(records_df['viewport_height'].median())
            target_dims = (median_width, median_height)
            print(f"target viewport: {target_dims[0]}x{target_dims[1]} (median-based)")
        
        elif method == 'adaptive':
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
    
    def process_full_dataset(self, df: pd.DataFrame, method: str = 'adaptive') -> pd.DataFrame:
        """
        main processing function that handles the complete pipeline
        """
        print("starting foolproof video preprocessing...")
        print(f"video aspect ratio: {self.video_aspect_ratio:.3f}")
        
        # extract and parse data
        records_df = self.extract_viewport_dimensions(df)
        print(f"successfully parsed {len(records_df)} records")
        
        if len(records_df) == 0:
            raise ValueError("no valid records found")
        
        # determine target viewport
        target_viewport = self.determine_target_viewport(records_df, method)
        
        # process each record
        processed_records = []
        total_original_points = 0
        total_valid_points = 0
        
        for idx, row in records_df.iterrows():
            try:
                transformation_result = self.filter_and_transform_gaze_coordinates(
                    row['raw_gaze_coords'],
                    row['viewport_width'],
                    row['viewport_height'],
                    target_viewport
                )
                
                total_original_points += transformation_result['total_points']
                total_valid_points += transformation_result['valid_points']
                
                processed_records.append({
                    'record_id': row['record_id'],
                    'user_id': row['user_id'],
                    'video_id': row['video_id'],
                    
                    # original viewport info
                    'original_viewport_width': row['viewport_width'],
                    'original_viewport_height': row['viewport_height'],
                    'original_viewport_aspect': row['viewport_aspect_ratio'],
                    
                    # target viewport info
                    'target_viewport_width': target_viewport[0],
                    'target_viewport_height': target_viewport[1],
                    'target_viewport_aspect': round(target_viewport[0]/target_viewport[1], 3),
                    
                    # video positioning in original viewport
                    'original_video_width': transformation_result['source_video_area'].video_width,
                    'original_video_height': transformation_result['source_video_area'].video_height,
                    'original_video_offset_x': transformation_result['source_video_area'].offset_x,
                    'original_video_offset_y': transformation_result['source_video_area'].offset_y,
                    'original_scale_factor': transformation_result['source_video_area'].scale_factor,
                    
                    # video positioning in target viewport
                    'target_video_width': transformation_result['target_video_area'].video_width,
                    'target_video_height': transformation_result['target_video_area'].video_height,
                    'target_video_offset_x': transformation_result['target_video_area'].offset_x,
                    'target_video_offset_y': transformation_result['target_video_area'].offset_y,
                    'target_scale_factor': transformation_result['target_video_area'].scale_factor,
                    
                    # gaze data statistics
                    'total_gaze_points': transformation_result['total_points'],
                    'valid_gaze_points': transformation_result['valid_points'],
                    'invalid_gaze_points': transformation_result['invalid_points'],
                    'retention_rate': transformation_result['retention_rate'],
                    
                    # transformed gaze coordinates (for yolo training)
                    'transformed_gaze_coords': transformation_result['transformed_coords'],
                    
                    # detailed coordinate info (for validation)
                    'detailed_coords': transformation_result['valid_coords'],
                    'invalid_coords': transformation_result['invalid_coords'],
                    
                    'form_data': row['form_data']
                })
                
            except Exception as e:
                print(f"error processing record {idx}: {e}")
                continue
        
        processed_df = pd.DataFrame(processed_records)
        
        # store processing statistics
        self.processing_stats = {
            'total_records': len(df),
            'parsed_records': len(records_df),
            'processed_records': len(processed_df),
            'total_original_gaze_points': total_original_points,
            'total_valid_gaze_points': total_valid_points,
            'overall_retention_rate': total_valid_points / total_original_points if total_original_points > 0 else 0,
            'target_viewport': target_viewport
        }
        
        print(f"\nprocessing complete:")
        print(f"  processed {len(processed_df)} records")
        print(f"  retained {total_valid_points:,} of {total_original_points:,} gaze points ({self.processing_stats['overall_retention_rate']:.1%})")
        print(f"  target viewport: {target_viewport[0]}x{target_viewport[1]}")
        
        return processed_df

class FilteringThresholdAnalyzer:
    def __init__(self, processed_df: pd.DataFrame):
        """
        initialize with processed dataframe from foolproof preprocessor
        """
        self.df = processed_df.copy()
        self.analysis_results = {}
        
    def extract_performance_metrics(self):
        """
        extract task performance metrics from form data
        """
        performance_data = []
        
        for idx, row in self.df.iterrows():
            try:
                form_data_str = str(row['form_data'])
                
                # parse form data
                cleaned_str = re.sub(r"ObjectId\([^)]+\)", '""', form_data_str)
                form_data = ast.literal_eval(cleaned_str)
                
                # extract key performance indicators
                hazard_detected = form_data.get('hazardDetected', False)
                if hazard_detected == 'True' or hazard_detected == True or hazard_detected == 'yes':
                    hazard_detected = True
                else:
                    hazard_detected = False
                    
                detection_confidence = int(form_data.get('detectionConfidence', 0))
                hazard_severity = int(form_data.get('hazardSeverity', 0))
                
                # spacebar timestamps indicate active hazard detection
                spacebar_timestamps = form_data.get('spacebarTimestamps', [])
                num_spacebar_presses = len(spacebar_timestamps) if spacebar_timestamps else 0
                
                # calculate reaction metrics if timestamps available
                start_time = form_data.get('startTime', 0)
                end_time = form_data.get('endTime', 0)
                session_duration = (end_time - start_time) / 1000 if end_time > start_time else 0
                
                performance_data.append({
                    'record_id': row['record_id'],
                    'user_id': row['user_id'],
                    'video_id': row['video_id'],
                    'retention_rate': row['retention_rate'],
                    'hazard_detected': hazard_detected,
                    'detection_confidence': detection_confidence,
                    'hazard_severity': hazard_severity,
                    'num_spacebar_presses': num_spacebar_presses,
                    'session_duration': session_duration,
                    'total_gaze_points': row['total_gaze_points'],
                    'valid_gaze_points': row['valid_gaze_points']
                })
                
            except Exception as e:
                print(f"error parsing form data for record {idx}: {e}")
                continue
        
        return pd.DataFrame(performance_data)
    
    def analyze_retention_distribution(self, performance_df: pd.DataFrame):
        """
        analyze the distribution of retention rates
        research basis: holmqvist et al. (2011) - eye tracking data quality standards
        """
        print("="*60)
        print("retention rate distribution analysis")
        print("="*60)
        
        retention_rates = performance_df['retention_rate']
        
        # basic statistics
        print(f"retention rate statistics:")
        print(f"  mean: {retention_rates.mean():.3f} ({retention_rates.mean()*100:.1f}%)")
        print(f"  median: {retention_rates.median():.3f} ({retention_rates.median()*100:.1f}%)")
        print(f"  std: {retention_rates.std():.3f}")
        print(f"  min: {retention_rates.min():.3f} ({retention_rates.min()*100:.1f}%)")
        print(f"  max: {retention_rates.max():.3f} ({retention_rates.max()*100:.1f}%)")
        
        # percentile analysis
        percentiles = [5, 10, 25, 50, 75, 90, 95]
        print(f"\npercentile analysis:")
        for p in percentiles:
            value = np.percentile(retention_rates, p)
            print(f"  {p}th percentile: {value:.3f} ({value*100:.1f}%)")
        
        # research-based thresholds
        print(f"\nresearch-based quality thresholds:")
        print(f"  holmqvist et al. (2011): >75% valid data recommended")
        print(f"  records meeting 75% threshold: {(retention_rates >= 0.75).sum()} ({(retention_rates >= 0.75).mean()*100:.1f}%)")
        print(f"  records meeting 70% threshold: {(retention_rates >= 0.70).sum()} ({(retention_rates >= 0.70).mean()*100:.1f}%)")
        print(f"  records meeting 60% threshold: {(retention_rates >= 0.60).sum()} ({(retention_rates >= 0.60).mean()*100:.1f}%)")
        
        self.analysis_results['retention_stats'] = {
            'mean': retention_rates.mean(),
            'median': retention_rates.median(),
            'std': retention_rates.std(),
            'percentiles': {p: np.percentile(retention_rates, p) for p in percentiles}
        }
        
        return retention_rates
    
    def analyze_performance_correlation(self, performance_df: pd.DataFrame):
        """
        analyze correlation between retention rate and task performance
        research basis: castro et al. (2014) - driving attention and performance correlation
        """
        print("\n" + "="*60)
        print("retention rate vs task performance correlation")
        print("="*60)
        
        # correlation with hazard detection accuracy
        hazard_detection_by_retention = performance_df.groupby('retention_rate')['hazard_detected'].mean()
        correlation_hazard, p_value_hazard = pearsonr(performance_df['retention_rate'], 
                                                     performance_df['hazard_detected'].astype(int))
        
        print(f"correlation with hazard detection:")
        print(f"  pearson r: {correlation_hazard:.3f} (p={p_value_hazard:.4f})")
        
        # correlation with detection confidence
        confidence_data = performance_df[performance_df['detection_confidence'] > 0]
        correlation_conf = None
        if len(confidence_data) > 10:
            correlation_conf, p_value_conf = pearsonr(confidence_data['retention_rate'], 
                                                     confidence_data['detection_confidence'])
            print(f"correlation with detection confidence:")
            print(f"  pearson r: {correlation_conf:.3f} (p={p_value_conf:.4f})")
        
        # correlation with spacebar activity (engagement)
        correlation_spacebar, p_value_spacebar = pearsonr(performance_df['retention_rate'], 
                                                         performance_df['num_spacebar_presses'])
        print(f"correlation with spacebar activity (engagement):")
        print(f"  pearson r: {correlation_spacebar:.3f} (p={p_value_spacebar:.4f})")
        
        # threshold analysis for performance
        thresholds = [0.5, 0.6, 0.7, 0.75, 0.8, 0.85, 0.9]
        print(f"\nperformance by retention threshold:")
        print(f"{'threshold':<10} {'n_records':<10} {'hazard_rate':<12} {'avg_confidence':<15} {'avg_spacebar':<12}")
        print("-" * 65)
        
        threshold_results = {}
        for threshold in thresholds:
            subset = performance_df[performance_df['retention_rate'] >= threshold]
            if len(subset) > 0:
                hazard_rate = subset['hazard_detected'].mean()
                avg_confidence = subset[subset['detection_confidence'] > 0]['detection_confidence'].mean()
                avg_spacebar = subset['num_spacebar_presses'].mean()
                
                print(f"{threshold:<10.2f} {len(subset):<10} {hazard_rate:<12.3f} {avg_confidence:<15.2f} {avg_spacebar:<12.2f}")
                
                threshold_results[threshold] = {
                    'n_records': len(subset),
                    'hazard_rate': hazard_rate,
                    'avg_confidence': avg_confidence if not np.isnan(avg_confidence) else 0,
                    'avg_spacebar': avg_spacebar
                }
        
        self.analysis_results['performance_correlations'] = {
            'hazard_correlation': correlation_hazard,
            'confidence_correlation': correlation_conf,
            'spacebar_correlation': correlation_spacebar,
            'threshold_analysis': threshold_results
        }
        
        return threshold_results
    
    def create_comprehensive_visualizations(self, performance_df: pd.DataFrame):
        """
        create visualizations to support threshold decision
        """
        fig, axes = plt.subplots(3, 3, figsize=(20, 15))
        
        # plot 1: retention rate distribution
        axes[0, 0].hist(performance_df['retention_rate'], bins=30, alpha=0.7, color='skyblue', edgecolor='black')
        axes[0, 0].axvline(0.75, color='red', linestyle='--', label='75% threshold')
        axes[0, 0].axvline(0.60, color='orange', linestyle='--', label='60% threshold')
        axes[0, 0].set_title('retention rate distribution')
        axes[0, 0].set_xlabel('retention rate')
        axes[0, 0].set_ylabel('frequency')
        axes[0, 0].legend()
        
        # plot 2: retention vs hazard detection
        retention_bins = pd.cut(performance_df['retention_rate'], bins=10)
        hazard_by_retention = performance_df.groupby(retention_bins)['hazard_detected'].mean()
        axes[0, 1].plot(range(len(hazard_by_retention)), hazard_by_retention.values, 'bo-')
        axes[0, 1].set_title('hazard detection rate by retention')
        axes[0, 1].set_xlabel('retention rate decile')
        axes[0, 1].set_ylabel('hazard detection rate')
        
        # plot 3: retention vs spacebar activity
        axes[0, 2].scatter(performance_df['retention_rate'], performance_df['num_spacebar_presses'], 
                          alpha=0.6, color='green')
        axes[0, 2].set_title('retention vs engagement (spacebar)')
        axes[0, 2].set_xlabel('retention rate')
        axes[0, 2].set_ylabel('spacebar presses')
        
        # plot 4: threshold impact on sample size
        thresholds = np.arange(0.1, 1.0, 0.05)
        sample_sizes = [len(performance_df[performance_df['retention_rate'] >= t]) for t in thresholds]
        axes[1, 0].plot(thresholds, sample_sizes, 'r-', linewidth=2)
        axes[1, 0].axhline(len(performance_df) * 0.5, color='gray', linestyle='--', label='50% of data')
        axes[1, 0].set_title('sample size vs retention threshold')
        axes[1, 0].set_xlabel('retention threshold')
        axes[1, 0].set_ylabel('remaining records')
        axes[1, 0].legend()
        
        # plot 5: performance quality by threshold
        performance_quality = []
        for t in thresholds:
            subset = performance_df[performance_df['retention_rate'] >= t]
            if len(subset) > 10:
                quality_score = (subset['hazard_detected'].mean() + 
                               subset['num_spacebar_presses'].mean() / 10)  # normalized
                performance_quality.append(quality_score)
            else:
                performance_quality.append(np.nan)
        
        axes[1, 1].plot(thresholds, performance_quality, 'purple', linewidth=2)
        axes[1, 1].set_title('performance quality vs threshold')
        axes[1, 1].set_xlabel('retention threshold')
        axes[1, 1].set_ylabel('composite quality score')
        
        # plot 6: confidence distribution by retention groups
        low_retention = performance_df[performance_df['retention_rate'] < 0.6]['detection_confidence']
        high_retention = performance_df[performance_df['retention_rate'] >= 0.75]['detection_confidence']
        
        axes[1, 2].hist([low_retention, high_retention], bins=15, alpha=0.7, 
                       label=['<60% retention', '≥75% retention'], color=['red', 'blue'])
        axes[1, 2].set_title('confidence by retention group')
        axes[1, 2].set_xlabel('detection confidence')
        axes[1, 2].set_ylabel('frequency')
        axes[1, 2].legend()
        
        # plot 7: session duration vs retention
        axes[2, 0].scatter(performance_df['retention_rate'], performance_df['session_duration'], 
                          alpha=0.6, color='orange')
        axes[2, 0].set_title('session duration vs retention')
        axes[2, 0].set_xlabel('retention rate')
        axes[2, 0].set_ylabel('session duration (seconds)')
        
        # plot 8: outlier identification
        outlier_threshold = 0.25
        outliers = performance_df[performance_df['retention_rate'] < outlier_threshold]
        normal = performance_df[performance_df['retention_rate'] >= outlier_threshold]
        
        axes[2, 1].scatter(normal['retention_rate'], normal['total_gaze_points'], 
                          alpha=0.6, color='blue', label='normal', s=20)
        axes[2, 1].scatter(outliers['retention_rate'], outliers['total_gaze_points'], 
                          alpha=0.8, color='red', label='outliers', s=30)
        axes[2, 1].set_title('outlier identification')
        axes[2, 1].set_xlabel('retention rate')
        axes[2, 1].set_ylabel('total gaze points')
        axes[2, 1].legend()
        
        # plot 9: recommended threshold visualization
        recommended_thresholds = [0.25, 0.60, 0.75]
        threshold_labels = ['quality control', 'basic competence', 'high quality']
        threshold_colors = ['red', 'orange', 'green']
        
        for i, (thresh, label, color) in enumerate(zip(recommended_thresholds, threshold_labels, threshold_colors)):
            count = len(performance_df[performance_df['retention_rate'] >= thresh])
            axes[2, 2].bar(i, count, color=color, alpha=0.7, label=f'{label} (≥{thresh:.0%})')
        
        axes[2, 2].set_title('records by threshold level')
        axes[2, 2].set_xlabel('threshold category')
        axes[2, 2].set_ylabel('number of records')
        axes[2, 2].set_xticks(range(len(threshold_labels)))
        axes[2, 2].set_xticklabels(threshold_labels, rotation=45, ha='right')
        axes[2, 2].legend()
        
        plt.tight_layout()
        
        # Save to eda_figures directory
        figures_dir = os.path.join(os.path.dirname(__file__), 'eda_figures')
        os.makedirs(figures_dir, exist_ok=True)
        plt.savefig(os.path.join(figures_dir, 'gaze_retention_filtering_threshold_analysis.png'), dpi=300, bbox_inches='tight')
        plt.show()
    
    def generate_recommendations(self, performance_df: pd.DataFrame):
        """
        generate evidence-based recommendations for filtering thresholds
        """
        print("\n" + "="*80)
        print("evidence-based filtering recommendations")
        print("="*80)
        
        # analyze the current data characteristics
        retention_mean = performance_df['retention_rate'].mean()
        retention_median = performance_df['retention_rate'].median()
        
        # count records at different thresholds
        counts_25 = len(performance_df[performance_df['retention_rate'] >= 0.25])
        counts_60 = len(performance_df[performance_df['retention_rate'] >= 0.60])
        counts_75 = len(performance_df[performance_df['retention_rate'] >= 0.75])
        
        print(f"current dataset characteristics:")
        print(f"  total records: {len(performance_df)}")
        print(f"  mean retention: {retention_mean:.3f} ({retention_mean*100:.1f}%)")
        print(f"  median retention: {retention_median:.3f} ({retention_median*100:.1f}%)")
        
        print(f"\nrecords remaining at different thresholds:")
        print(f"  ≥25% retention: {counts_25} ({counts_25/len(performance_df)*100:.1f}%) - removes calibration failures")
        print(f"  ≥60% retention: {counts_60} ({counts_60/len(performance_df)*100:.1f}%) - basic driving competence")
        print(f"  ≥75% retention: {counts_75} ({counts_75/len(performance_df)*100:.1f}%) - high quality data")
        
        # performance-based recommendations
        if 'performance_correlations' in self.analysis_results:
            hazard_corr = self.analysis_results['performance_correlations']['hazard_correlation']
            if hazard_corr > 0.2:
                print(f"\nperformance correlation analysis:")
                print(f"  retention-hazard correlation: {hazard_corr:.3f} (significant)")
                print("  → higher retention rates associate with better task performance")
            else:
                print(f"\nperformance correlation analysis:")
                print(f"  retention-hazard correlation: {hazard_corr:.3f} (weak)")
                print("  → retention rate weakly predicts task performance")
        
        print(f"\nresearch-backed recommendations:")
        print(f"1. quality control threshold: 25%")
        print(f"   - removes clear calibration failures and disengaged participants")
        print(f"   - based on holmqvist et al. (2011) data quality standards")
        print(f"   - retains {counts_25} records ({counts_25/len(performance_df)*100:.1f}%)")
        
        print(f"\n2. moderate filtering threshold: 60-70%")
        print(f"   - ensures basic driving attention (chapman & underwood 1998)")
        print(f"   - balances data quality with sample size")
        print(f"   - retains {counts_60} records ({counts_60/len(performance_df)*100:.1f}%)")
        
        print(f"\n3. high quality threshold: 75%+")
        print(f"   - matches holmqvist et al. (2011) recommendations")
        print(f"   - ensures expert-level attention patterns")
        print(f"   - retains {counts_75} records ({counts_75/len(performance_df)*100:.1f}%)")
        
        # final recommendation based on data characteristics
        if retention_median >= 0.75:
            recommended_threshold = 0.75
            rationale = "high median retention allows strict filtering"
        elif retention_median >= 0.60:
            recommended_threshold = 0.60
            rationale = "moderate median retention suggests 60% threshold"
        else:
            recommended_threshold = 0.25
            rationale = "low median retention requires liberal threshold"
        
        print(f"\nrecommended threshold for your dataset: {recommended_threshold:.0%}")
        print(f"rationale: {rationale}")
        print(f"this would retain {len(performance_df[performance_df['retention_rate'] >= recommended_threshold])} records")
        
        return recommended_threshold

def main():
    """
    run the complete gaze retention analysis and filtering threshold determination
    """
    print("="*80)
    print("complete gaze data analysis and filtering threshold determination")
    print("="*80)
    
    # Create output directories
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, 'eda_data')
    figures_dir = os.path.join(script_dir, 'eda_figures')
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(figures_dir, exist_ok=True)
    
    # load raw data
    try:
        raw_df = pd.read_csv('../data/raw/survey_results_raw.csv')
        print(f"loaded {len(raw_df)} raw records")
    except FileNotFoundError:
        print("error: ../data/raw/survey_results_raw.csv not found")
        print("please ensure the file path is correct")
        return None, None
    
    # run preprocessing
    print("\nrunning preprocessing...")
    preprocessor = FoolproofVideoPreprocessor(video_aspect_ratio=1280/960)
    processed_df = preprocessor.process_full_dataset(raw_df, method='adaptive')
    
    # save processed data to eda_data directory
    processed_data_path = os.path.join(data_dir, 'gaze_retention_processed_data.csv')
    processed_df.to_csv(processed_data_path, index=False)
    print(f"saved processed data to: {processed_data_path}")
    
    # run filtering analysis
    print("\nrunning filtering threshold analysis...")
    analyzer = FilteringThresholdAnalyzer(processed_df)
    
    # extract performance metrics
    performance_df = analyzer.extract_performance_metrics()
    print(f"extracted performance data for {len(performance_df)} records")
    
    if len(performance_df) == 0:
        print("error: no performance data could be extracted")
        return processed_df, None
    
    # run comprehensive analysis
    print("\nstarting comprehensive analysis...")
    
    # 1. distribution analysis
    retention_rates = analyzer.analyze_retention_distribution(performance_df)
    
    # 2. performance correlation analysis  
    threshold_results = analyzer.analyze_performance_correlation(performance_df)
    
    # 3. create visualizations
    analyzer.create_comprehensive_visualizations(performance_df)
    
    # 4. generate final recommendations
    recommended_threshold = analyzer.generate_recommendations(performance_df)
    
    # save detailed results to eda_data directory
    results_summary = {
        'analysis_date': pd.Timestamp.now().isoformat(),
        'total_records_analyzed': len(performance_df),
        'recommended_threshold': recommended_threshold,
        'retention_statistics': analyzer.analysis_results.get('retention_stats', {}),
        'performance_correlations': analyzer.analysis_results.get('performance_correlations', {}),
        'preprocessing_stats': preprocessor.processing_stats
    }
    
    results_path = os.path.join(data_dir, 'gaze_retention_analysis_results.json')
    with open(results_path, 'w') as f:
        json.dump(results_summary, f, indent=2)
    
    print(f"\n" + "="*80)
    print("analysis complete!")
    print("="*80)
    print(f"saved files:")
    print(f"  - {processed_data_path} (processed data)")
    print(f"  - {os.path.join(figures_dir, 'gaze_retention_filtering_threshold_analysis.png')} (visualizations)")
    print(f"  - {results_path} (detailed results)")
    
    return processed_df, recommended_threshold

if __name__ == "__main__":
    processed_data, recommended_threshold = main()