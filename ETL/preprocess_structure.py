import pandas as pd
import numpy as np
import json
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

@dataclass
class GazePoint:
    """represents a single gaze point with coordinates and time"""
    x: float
    y: float
    time: float
    video_rel_x: Optional[float] = None
    video_rel_y: Optional[float] = None
    coordinate_type: str = 'video_area'

class HazardDatasetCreator:
    def __init__(self, 
                 spacebar_window_ms: int = 2000,
                 gaze_window_ms: int = 500,
                 min_gaze_points: int = 3):
        """
        initialize the dataset creator
        
        Args:
            spacebar_window_ms: time window around spacebar press to consider for hazard
            gaze_window_ms: time window for aggregating gaze data
            min_gaze_points: minimum gaze points needed for reliable metrics
        """
        self.spacebar_window_ms = spacebar_window_ms
        self.gaze_window_ms = gaze_window_ms
        self.min_gaze_points = min_gaze_points
        self.session_counter = defaultdict(lambda: defaultdict(int))
        
    def restructure_csv(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        main function to restructure the csv for hazard detection training
        """
        print(f"processing {len(df)} records...")
        
        # track unique sessions
        self._build_session_numbers(df)
        
        # process each record
        all_rows = []
        
        for idx, row in df.iterrows():
            if idx % 10 == 0:
                print(f"processing record {idx + 1}/{len(df)}")
            
            try:
                processed_rows = self._process_single_record(row)
                all_rows.extend(processed_rows)
            except Exception as e:
                print(f"error processing record {idx}: {e}")
                continue
        
        # create dataframe
        result_df = pd.DataFrame(all_rows)
        
        # add derived features
        result_df = self._add_derived_features(result_df)
        
        print(f"created {len(result_df)} training samples from {len(df)} records")
        
        return result_df
    
    def _build_session_numbers(self, df: pd.DataFrame):
        """
        build session numbers for each unique user-video combination
        """
        # sort by user_id, video_id, and any timestamp to ensure consistent ordering
        df_sorted = df.sort_values(['user_id', 'video_id'])
        
        for _, row in df_sorted.iterrows():
            user_id = row['user_id']
            video_id = row['video_id']
            self.session_counter[user_id][video_id] += 1
    
    def _process_single_record(self, row: pd.Series) -> List[Dict]:
        """
        process a single record to extract timestamp-level data
        """
        # get session number
        user_id = row['user_id']
        video_id = row['video_id']
        session_num = self._get_session_number(user_id, video_id)
        
        # parse json data
        try:
            all_coords = json.loads(row['all_transformed_coords']) if isinstance(row['all_transformed_coords'], str) else row['all_transformed_coords']
            video_coords = json.loads(row['video_coords_only']) if isinstance(row['video_coords_only'], str) else row['video_coords_only']
            spacebar_times = json.loads(row['spacebar_timestamps']) if isinstance(row['spacebar_timestamps'], str) else row['spacebar_timestamps']
        except (json.JSONDecodeError, TypeError) as e:
            print(f"error parsing json for record {row['record_id']}: {e}")
            return []
        
        # ensure spacebar_times is a list
        if not isinstance(spacebar_times, list):
            spacebar_times = []
        
        processed_rows = []
        
        # process each gaze point
        for i, coord in enumerate(all_coords):
            # calculate temporal features
            temporal_features = self._calculate_temporal_features(
                coord, i, all_coords, spacebar_times
            )
            
            # calculate spatial features
            spatial_features = self._calculate_spatial_features(
                coord, i, all_coords
            )
            
            # calculate attention features
            attention_features = self._calculate_attention_features(
                coord, i, all_coords, self.gaze_window_ms
            )
            
            # determine if this point is near a hazard event
            is_hazard_moment = self._is_hazard_moment(
                coord['time'], 
                spacebar_times, 
                row.get('hazard_detected', False)
            )
            
            # create row
            new_row = {
                # identifiers
                'record_id': row['record_id'],
                'user_id': user_id,
                'video_id': video_id,
                'session_num': session_num,
                'timestamp': coord['time'],
                'frame_index': i,
                
                # basic gaze data
                'gaze_x': coord.get('x', np.nan),
                'gaze_y': coord.get('y', np.nan),
                'video_rel_x': coord.get('video_rel_x', np.nan),
                'video_rel_y': coord.get('video_rel_y', np.nan),
                'coordinate_type': coord.get('coordinate_type', 'unknown'),
                
                # viewport info (for context)
                'viewport_width': row['target_viewport_width'],
                'viewport_height': row['target_viewport_height'],
                
                # temporal features
                **temporal_features,
                
                # spatial features
                **spatial_features,
                
                # attention features
                **attention_features,
                
                # hazard labels
                'hazard_detected_session': row.get('hazard_detected', False),
                'is_hazard_moment': is_hazard_moment,
                'detection_confidence': row.get('detection_confidence', 0),
                'hazard_severity': row.get('hazard_severity', 0),
                
                # session metrics
                'session_duration': row.get('session_duration', 0),
                'total_spacebar_presses': row.get('num_spacebar_presses', 0)
            }
            
            processed_rows.append(new_row)
        
        return processed_rows
    
    def _get_session_number(self, user_id: str, video_id: str) -> int:
        """
        get the session number for a user-video combination
        """
        # this returns the count we built earlier
        return self.session_counter[user_id].get(video_id, 1)
    
    def _calculate_temporal_features(self, 
                                    current_coord: Dict,
                                    index: int,
                                    all_coords: List[Dict],
                                    spacebar_times: List) -> Dict:
        """
        calculate temporal features for a gaze point
        """
        features = {}
        current_time = current_coord['time']
        
        # time to nearest spacebar press
        if spacebar_times:
            time_diffs = [abs(current_time - t) for t in spacebar_times]
            features['time_to_nearest_spacebar'] = min(time_diffs)
            features['time_to_next_spacebar'] = min([t - current_time for t in spacebar_times if t > current_time], default=np.nan)
            features['time_since_last_spacebar'] = min([current_time - t for t in spacebar_times if t < current_time], default=np.nan)
        else:
            features['time_to_nearest_spacebar'] = np.nan
            features['time_to_next_spacebar'] = np.nan
            features['time_since_last_spacebar'] = np.nan
        
        # velocity features
        if index > 0:
            prev_coord = all_coords[index - 1]
            time_diff = current_time - prev_coord['time']
            if time_diff > 0:
                features['gaze_velocity_x'] = (current_coord.get('x', 0) - prev_coord.get('x', 0)) / time_diff
                features['gaze_velocity_y'] = (current_coord.get('y', 0) - prev_coord.get('y', 0)) / time_diff
                features['gaze_speed'] = np.sqrt(features['gaze_velocity_x']**2 + features['gaze_velocity_y']**2)
            else:
                features['gaze_velocity_x'] = 0
                features['gaze_velocity_y'] = 0
                features['gaze_speed'] = 0
        else:
            features['gaze_velocity_x'] = 0
            features['gaze_velocity_y'] = 0
            features['gaze_speed'] = 0
        
        # acceleration features
        if index > 1:
            prev_velocity_x = features['gaze_velocity_x']
            prev_velocity_y = features['gaze_velocity_y']
            prev_prev_coord = all_coords[index - 2]
            prev_coord = all_coords[index - 1]
            prev_time_diff = prev_coord['time'] - prev_prev_coord['time']
            
            if prev_time_diff > 0:
                prev_prev_velocity_x = (prev_coord.get('x', 0) - prev_prev_coord.get('x', 0)) / prev_time_diff
                prev_prev_velocity_y = (prev_coord.get('y', 0) - prev_prev_coord.get('y', 0)) / prev_time_diff
                
                time_diff = current_time - prev_coord['time']
                if time_diff > 0:
                    features['gaze_acceleration_x'] = (prev_velocity_x - prev_prev_velocity_x) / time_diff
                    features['gaze_acceleration_y'] = (prev_velocity_y - prev_prev_velocity_y) / time_diff
                else:
                    features['gaze_acceleration_x'] = 0
                    features['gaze_acceleration_y'] = 0
            else:
                features['gaze_acceleration_x'] = 0
                features['gaze_acceleration_y'] = 0
        else:
            features['gaze_acceleration_x'] = 0
            features['gaze_acceleration_y'] = 0
        
        return features
    
    def _calculate_spatial_features(self,
                                   current_coord: Dict,
                                   index: int,
                                   all_coords: List[Dict]) -> Dict:
        """
        calculate spatial features for a gaze point
        """
        features = {}
        
        # position in normalized coordinates
        video_x = current_coord.get('video_rel_x', np.nan)
        video_y = current_coord.get('video_rel_y', np.nan)
        
        if not pd.isna(video_x) and not pd.isna(video_y):
            # quadrant (1-4, like mathematical quadrants but for screen)
            if video_x < 0.5 and video_y < 0.5:
                features['screen_quadrant'] = 1  # top-left
            elif video_x >= 0.5 and video_y < 0.5:
                features['screen_quadrant'] = 2  # top-right
            elif video_x < 0.5 and video_y >= 0.5:
                features['screen_quadrant'] = 3  # bottom-left
            else:
                features['screen_quadrant'] = 4  # bottom-right
            
            # distance from center
            features['distance_from_center'] = np.sqrt((video_x - 0.5)**2 + (video_y - 0.5)**2)
            
            # position zones (useful for road scenes)
            features['is_center_region'] = 1 if (0.3 < video_x < 0.7 and 0.3 < video_y < 0.7) else 0
            features['is_peripheral'] = 1 if (video_x < 0.2 or video_x > 0.8 or video_y < 0.2 or video_y > 0.8) else 0
            features['is_upper_half'] = 1 if video_y < 0.5 else 0
            features['is_lower_half'] = 1 if video_y >= 0.5 else 0
            features['is_left_half'] = 1 if video_x < 0.5 else 0
            features['is_right_half'] = 1 if video_x >= 0.5 else 0
            
            # road-specific zones (assuming typical dashboard camera view)
            features['is_horizon_region'] = 1 if (0.3 < video_y < 0.5) else 0
            features['is_dashboard_region'] = 1 if video_y > 0.8 else 0
            features['is_sky_region'] = 1 if video_y < 0.2 else 0
        else:
            # default values for missing coordinates
            features['screen_quadrant'] = 0
            features['distance_from_center'] = np.nan
            features['is_center_region'] = 0
            features['is_peripheral'] = 0
            features['is_upper_half'] = 0
            features['is_lower_half'] = 0
            features['is_left_half'] = 0
            features['is_right_half'] = 0
            features['is_horizon_region'] = 0
            features['is_dashboard_region'] = 0
            features['is_sky_region'] = 0
        
        return features
    
    def _calculate_attention_features(self,
                                     current_coord: Dict,
                                     index: int,
                                     all_coords: List[Dict],
                                     window_ms: int) -> Dict:
        """
        calculate attention-based features from gaze patterns
        """
        features = {}
        current_time = current_coord['time']
        
        # get gaze points within time window
        window_coords = []
        for coord in all_coords:
            if abs(coord['time'] - current_time) <= window_ms:
                if coord.get('video_rel_x') is not None and coord.get('video_rel_y') is not None:
                    window_coords.append(coord)
        
        if len(window_coords) >= self.min_gaze_points:
            # calculate dispersion (how spread out the gaze is)
            x_coords = [c.get('video_rel_x', 0) for c in window_coords]
            y_coords = [c.get('video_rel_y', 0) for c in window_coords]
            
            features['gaze_dispersion_x'] = np.std(x_coords)
            features['gaze_dispersion_y'] = np.std(y_coords)
            features['gaze_dispersion_total'] = np.sqrt(features['gaze_dispersion_x']**2 + features['gaze_dispersion_y']**2)
            
            # fixation detection (low dispersion = fixation)
            fixation_threshold = 0.05  # 5% of screen
            features['is_fixation'] = 1 if features['gaze_dispersion_total'] < fixation_threshold else 0
            
            # saccade detection (high velocity between points)
            if index > 0:
                features['is_saccade'] = 1 if features.get('gaze_speed', 0) > 0.5 else 0
            else:
                features['is_saccade'] = 0
            
            # smooth pursuit (moderate, consistent velocity)
            if len(window_coords) >= 3:
                velocities = []
                for i in range(1, len(window_coords)):
                    time_diff = window_coords[i]['time'] - window_coords[i-1]['time']
                    if time_diff > 0:
                        vx = (window_coords[i]['video_rel_x'] - window_coords[i-1]['video_rel_x']) / time_diff
                        vy = (window_coords[i]['video_rel_y'] - window_coords[i-1]['video_rel_y']) / time_diff
                        velocities.append(np.sqrt(vx**2 + vy**2))
                
                if velocities:
                    velocity_std = np.std(velocities)
                    mean_velocity = np.mean(velocities)
                    features['is_smooth_pursuit'] = 1 if (0.1 < mean_velocity < 0.5 and velocity_std < 0.1) else 0
                else:
                    features['is_smooth_pursuit'] = 0
            else:
                features['is_smooth_pursuit'] = 0
        else:
            # not enough data points
            features['gaze_dispersion_x'] = np.nan
            features['gaze_dispersion_y'] = np.nan
            features['gaze_dispersion_total'] = np.nan
            features['is_fixation'] = 0
            features['is_saccade'] = 0
            features['is_smooth_pursuit'] = 0
        
        return features
    
    def _is_hazard_moment(self, 
                         timestamp: float,
                         spacebar_times: List,
                         hazard_detected: bool) -> bool:
        """
        determine if this timestamp is during a hazard event
        """
        if not hazard_detected:
            return False
        
        if not spacebar_times:
            return False
        
        # check if timestamp is within window of any spacebar press
        for spacebar_time in spacebar_times:
            if abs(timestamp - spacebar_time) <= self.spacebar_window_ms:
                return True
        
        return False
    
    def _add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        add derived features that require the full dataset
        """
        # add rolling statistics
        df = df.sort_values(['user_id', 'video_id', 'session_num', 'timestamp'])
        
        # rolling mean of gaze positions (smoothing)
        window_size = 5
        for col in ['video_rel_x', 'video_rel_y']:
            if col in df.columns:
                df[f'{col}_rolling_mean'] = df.groupby(['user_id', 'video_id', 'session_num'])[col].transform(
                    lambda x: x.rolling(window_size, center=True, min_periods=1).mean()
                )
        
        # time since session start
        df['time_since_start'] = df.groupby(['user_id', 'video_id', 'session_num'])['timestamp'].transform(
            lambda x: x - x.min()
        )
        
        # percentage through video (normalized time)
        df['video_progress'] = df.groupby(['user_id', 'video_id', 'session_num'])['time_since_start'].transform(
            lambda x: x / x.max() if x.max() > 0 else 0
        )
        
        # add interaction features
        df['velocity_x_accel_x'] = df['gaze_velocity_x'] * df['gaze_acceleration_x']
        df['velocity_y_accel_y'] = df['gaze_velocity_y'] * df['gaze_acceleration_y']
        
        # add categorical encoding for coordinate type
        df['is_video_gaze'] = (df['coordinate_type'] == 'video_area').astype(int)
        
        return df
    
    def create_training_labels(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        create final training labels using gaze patterns to identify hazards
        
        this is where we would integrate with yolo detections to assign
        hazard labels to specific objects based on gaze attention
        """
        # for now, create basic hazard labels
        df['hazard_label'] = 0
        
        # mark hazard moments with high confidence
        hazard_mask = (
            (df['is_hazard_moment'] == True) & 
            (df['is_video_gaze'] == 1) &
            (df['is_fixation'] == 1)  # focused gaze during hazard
        )
        df.loc[hazard_mask, 'hazard_label'] = 1
        
        # add confidence score based on gaze patterns
        df['hazard_confidence'] = 0.0
        
        # high confidence: fixation near spacebar press
        high_conf_mask = (
            (df['time_to_nearest_spacebar'] < 500) & 
            (df['is_fixation'] == 1) &
            (df['hazard_detected_session'] == True)
        )
        df.loc[high_conf_mask, 'hazard_confidence'] = 1.0
        
        # medium confidence: gaze in area near spacebar press
        med_conf_mask = (
            (df['time_to_nearest_spacebar'] < 1000) & 
            (df['hazard_detected_session'] == True) &
            (df['is_video_gaze'] == 1)
        )
        df.loc[med_conf_mask, 'hazard_confidence'] = df.loc[med_conf_mask, 'hazard_confidence'].clip(lower=0.5)
        
        return df

# main execution function
def process_gaze_data(input_csv_path: str, output_csv_path: str):
    """
    main function to process the gaze data csv
    
    Args:
        input_csv_path: path to input csv with scaled gaze data
        output_csv_path: path to save restructured csv
    """
    # load data
    print(f"loading data from {input_csv_path}")
    df = pd.read_csv(input_csv_path)
    
    # create processor
    processor = HazardDatasetCreator(
        spacebar_window_ms=2000,
        gaze_window_ms=500,
        min_gaze_points=3
    )
    
    # restructure data
    print("restructuring data...")
    restructured_df = processor.restructure_csv(df)
    
    # create training labels
    print("creating training labels...")
    final_df = processor.create_training_labels(restructured_df)
    
    # save to csv
    print(f"saving to {output_csv_path}")
    final_df.to_csv(output_csv_path, index=False)
    
    # print summary statistics
    print("\n" + "="*50)
    print("processing complete!")
    print("="*50)
    print(f"total samples: {len(final_df)}")
    print(f"unique users: {final_df['user_id'].nunique()}")
    print(f"unique videos: {final_df['video_id'].nunique()}")
    print(f"hazard moments: {final_df['is_hazard_moment'].sum()}")
    print(f"hazard labels: {final_df['hazard_label'].sum()}")
    print(f"columns created: {len(final_df.columns)}")
    
    # show sample of features
    print("\nfeature columns:")
    feature_cols = [col for col in final_df.columns if col not in ['record_id', 'user_id', 'video_id', 'timestamp']]
    for i in range(0, len(feature_cols), 4):
        print("  " + ", ".join(feature_cols[i:i+4]))
    
    return final_df

# unified pipeline usage
if __name__ == "__main__":
    print("="*80)
    print("structure preprocessing (heavy compute) - unified pipeline")
    print("="*80)
    
    # import unified preprocessing utilities
    from preprocessing_utils import get_preprocessing_utils
    utils = get_preprocessing_utils()
    
    # load output from previous step (reaction_time)
    input_df = utils.load_previous_output('reaction_time', 'results')
    if input_df.empty:
        print("error: no input data found from previous step")
        exit(1)
    
    print(f"loaded {len(input_df)} records from previous step")
    
    # create temporary input file for processing
    temp_input = "/tmp/temp_input.csv"
    temp_output = "/tmp/temp_output.csv"
    input_df.to_csv(temp_input, index=False)
    
    # process the data (heavy compute step)
    result_df = process_gaze_data(temp_input, temp_output)
    
    # save to unified S3 structure
    saved_keys = utils.save_unified_output(
        result_df, 
        data_type='results',
        script_name='structure'
    )
    
    print(f"\n" + "="*80)
    print("structure preprocessing complete!")
    print("="*80)
    print(f"output files saved to S3:")
    for key in saved_keys[:3]:
        print(f"  - s3://{utils.bucket}/{key}")
    if len(saved_keys) > 3:
        print(f"  - ... and {len(saved_keys) - 3} more files")
    
    print(f"\nstructured data ready for bounding box analysis!")