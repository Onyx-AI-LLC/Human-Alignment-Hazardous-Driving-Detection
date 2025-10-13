import pandas as pd
import numpy as np
import json
import ast
from typing import Dict, List

class ReactionTimeAdjuster:
    def __init__(self, gaze_csv_path: str, users_csv_path: str):
        """
        initialize with paths to gaze data and user demographics
        
        Args:
            gaze_csv_path: path to screen&gaze_scaled_last15s.csv
            users_csv_path: path to users_data_raw.csv with demographics
        """
        print("loading data files...")
        self.gaze_df = pd.read_csv(gaze_csv_path)
        self.users_df = pd.read_csv(users_csv_path)
        
        print(f"loaded {len(self.gaze_df)} gaze records")
        print(f"loaded {len(self.users_df)} user profiles")
        
        # parse user demographics
        self.user_demographics = self._parse_user_demographics()
        
        # statistics tracking
        self.stats = {
            'total_records': len(self.gaze_df),
            'records_with_spacebar': 0,
            'timestamps_adjusted': 0,
            'avg_adjustment_ms': [],
            'users_missing_demographics': set()
        }
    
    def _parse_user_demographics(self) -> Dict:
        """
        parse user demographics from the user data fields
        """
        demographics = {}
        
        for _, row in self.users_df.iterrows():
            user_email = row['email']
            
            try:
                # extract age directly from user data
                age = row.get('age', None)
                if pd.notna(age):
                    age = int(age)
                else:
                    age = None
                
                # extract gender directly from user data
                gender = row.get('gender', None)
                if pd.isna(gender):
                    gender = None
                
                demographics[user_email] = {
                    'age': age,
                    'gender': gender
                }
                
            except Exception as e:
                print(f"error parsing demographics for {user_email}: {e}")
                demographics[user_email] = {'age': None, 'gender': None}
        
        return demographics
    
    def calculate_reaction_adjustment(self, age: int, gender: str) -> float:
        """
        calculate reaction time adjustment in milliseconds
        
        based on research: RT = Age × 2.8ms with gender adjustments:
        - Males: -50ms (faster)
        - Females: 0ms (baseline)
        - Other/Unknown: -25ms (average of male and female)
        
        Args:
            age: user's age in years
            gender: user's gender
        
        Returns:
            adjustment in milliseconds to subtract from timestamp
        """
        if age is None:
            # use average age of 30 if missing
            age = 30
        
        # base adjustment: 2.8ms per year of age
        adjustment = age * 2.8
        
        # gender adjustment based on research
        if gender and gender.lower() in ['male', 'm', 'man']:
            # males are 50ms faster
            adjustment -= 50
        elif gender and gender.lower() in ['female', 'f', 'woman']:
            # females are baseline (no adjustment)
            adjustment -= 0
        else:
            # for prefer-not-to-say, non-binary, or unknown: use average
            # average of male (-50) and female (0) = -25
            adjustment -= 25
        
        # minimum adjustment of 50ms (even for young males)
        # maximum adjustment of 300ms (for safety)
        adjustment = max(50, min(300, adjustment))
        
        return adjustment
    
    def adjust_spacebar_timestamps(self, spacebar_times: List[float], 
                                  adjustment_ms: float,
                                  video_end_time: float = None) -> List[float]:
        """
        adjust spacebar timestamps accounting for reaction time
        
        Args:
            spacebar_times: list of spacebar timestamps
            adjustment_ms: milliseconds to subtract
            video_end_time: last timestamp in video (to check if ending was automatic)
        
        Returns:
            adjusted timestamps
        """
        if not spacebar_times:
            return []
        
        adjusted = []
        
        for i, timestamp in enumerate(spacebar_times):
            if i % 2 == 0:  # odd-indexed (1st, 3rd, 5th...) - START timestamps
                # always adjust start timestamps
                adjusted_time = timestamp - adjustment_ms
                # ensure not negative
                adjusted_time = max(0, adjusted_time)
                adjusted.append(adjusted_time)
                
            else:  # even-indexed (2nd, 4th, 6th...) - END timestamps
                # check if this is at video end (automatic ending)
                if video_end_time and abs(timestamp - video_end_time) < 100:  # within 100ms of end
                    # don't adjust automatic endings
                    adjusted.append(timestamp)
                else:
                    # user manually ended - apply adjustment
                    adjusted_time = timestamp - adjustment_ms
                    # ensure not before previous start
                    if adjusted and adjusted_time > adjusted[-1]:
                        adjusted.append(adjusted_time)
                    else:
                        # keep original if adjustment would break sequence
                        adjusted.append(timestamp)
        
        return adjusted
    
    def process_dataset(self) -> pd.DataFrame:
        """
        process all records to apply reaction time adjustments
        """
        print("\n" + "="*50)
        print("applying reaction time adjustments")
        print("="*50)
        
        processed_records = []
        examples_to_print = []  # collect examples for display
        
        for idx, row in self.gaze_df.iterrows():
            if idx % 50 == 0:
                print(f"processing record {idx}/{len(self.gaze_df)}...")
            
            try:
                # get user demographics
                user_email = row['user_id']
                user_demo = self.user_demographics.get(user_email, {})
                
                if not user_demo or user_demo['age'] is None:
                    self.stats['users_missing_demographics'].add(user_email)
                
                age = user_demo.get('age', 30)  # default to 30 if missing
                gender = user_demo.get('gender', 'unknown')
                
                # calculate adjustment
                adjustment_ms = self.calculate_reaction_adjustment(age, gender)
                self.stats['avg_adjustment_ms'].append(adjustment_ms)
                
                # parse spacebar timestamps
                spacebar_times = json.loads(row.get('spacebar_timestamps', '[]'))
                
                if spacebar_times:
                    self.stats['records_with_spacebar'] += 1
                    
                    # get video end time for checking automatic endings
                    all_coords = json.loads(row.get('all_transformed_coords', '[]'))
                    video_end_time = None
                    if all_coords:
                        times = [c.get('time', 0) for c in all_coords]
                        video_end_time = max(times) if times else None
                    
                    # adjust timestamps
                    original_times = spacebar_times.copy()
                    adjusted_times = self.adjust_spacebar_timestamps(
                        spacebar_times, adjustment_ms, video_end_time
                    )
                    
                    # track statistics
                    if original_times != adjusted_times:
                        self.stats['timestamps_adjusted'] += len([i for i in range(len(original_times)) 
                                                                 if original_times[i] != adjusted_times[i]])
                    
                    # collect example for printing (first 5 with adjustments)
                    if len(examples_to_print) < 5 and original_times != adjusted_times:
                        examples_to_print.append({
                            'record_id': row['record_id'],
                            'user_id': user_email,
                            'age': age,
                            'gender': gender,
                            'adjustment_ms': adjustment_ms,
                            'original': original_times,
                            'adjusted': adjusted_times
                        })
                    
                    # update record
                    new_row = row.copy()
                    new_row['spacebar_timestamps'] = json.dumps(adjusted_times)
                    new_row['reaction_adjustment_ms'] = adjustment_ms
                    new_row['user_age'] = age
                    new_row['user_gender'] = gender
                else:
                    # no spacebar timestamps
                    new_row = row.copy()
                    new_row['reaction_adjustment_ms'] = 0
                    new_row['user_age'] = age
                    new_row['user_gender'] = gender
                
                processed_records.append(new_row)
                
            except Exception as e:
                print(f"error processing record {idx}: {e}")
                processed_records.append(row)
                continue
        
        # create processed dataframe
        processed_df = pd.DataFrame(processed_records)
        
        # print statistics
        self._print_statistics()
        
        # print examples
        self._print_examples(examples_to_print)
        
        return processed_df
    
    def _print_statistics(self):
        """
        print processing statistics
        """
        print("\n" + "="*50)
        print("reaction time adjustment statistics")
        print("="*50)
        
        print(f"total records processed: {self.stats['total_records']}")
        print(f"records with spacebar data: {self.stats['records_with_spacebar']}")
        print(f"total timestamps adjusted: {self.stats['timestamps_adjusted']}")
        
        if self.stats['avg_adjustment_ms']:
            adjustments = np.array(self.stats['avg_adjustment_ms'])
            print(f"\nadjustment statistics (ms):")
            print(f"  mean: {adjustments.mean():.1f} ms")
            print(f"  median: {np.median(adjustments):.1f} ms")
            print(f"  min: {adjustments.min():.1f} ms")
            print(f"  max: {adjustments.max():.1f} ms")
        
        if self.stats['users_missing_demographics']:
            print(f"\nusers missing demographics: {len(self.stats['users_missing_demographics'])}")
            print("  (defaulted to age 30 for these users)")
    
    def _print_examples(self, examples: List[Dict]):
        """
        print examples of adjusted timestamps
        """
        print("\n" + "="*50)
        print("examples of adjusted spacebar timestamps")
        print("="*50)
        
        for i, example in enumerate(examples, 1):
            print(f"\nexample {i}:")
            print(f"  user: {example['user_id']}")
            print(f"  age: {example['age']}, gender: {example['gender']}")
            print(f"  adjustment: -{example['adjustment_ms']:.0f} ms")
            
            print(f"  original timestamps:")
            for j, ts in enumerate(example['original']):
                label = "START" if j % 2 == 0 else "END"
                print(f"    [{j}] {ts:.0f} ms ({label})")
            
            print(f"  adjusted timestamps:")
            for j, (orig, adj) in enumerate(zip(example['original'], example['adjusted'])):
                label = "START" if j % 2 == 0 else "END"
                diff = orig - adj
                if diff > 0:
                    print(f"    [{j}] {adj:.0f} ms ({label}) [adjusted by -{diff:.0f} ms]")
                else:
                    print(f"    [{j}] {adj:.0f} ms ({label}) [no adjustment]")
    
    def save_adjusted_data(self, processed_df: pd.DataFrame, output_path: str = None):
        """
        save the adjusted dataframe
        """
        if output_path is None:
            output_path = "data/processed/screen&gaze_scaled_last15s_adjusted.csv"
        
        processed_df.to_csv(output_path, index=False)
        print(f"\nsaved adjusted data to: {output_path}")
        
        # save adjustment report
        report_path = output_path.replace('.csv', '_adjustment_report.txt')
        with open(report_path, 'w') as f:
            f.write("reaction time adjustment report\n")
            f.write("="*50 + "\n\n")
            f.write("adjustment formula: Age × 2.8ms - 50ms (if male)\n\n")
            
            f.write("statistics:\n")
            f.write(f"  records processed: {self.stats['total_records']}\n")
            f.write(f"  timestamps adjusted: {self.stats['timestamps_adjusted']}\n")
            
            if self.stats['avg_adjustment_ms']:
                adjustments = np.array(self.stats['avg_adjustment_ms'])
                f.write(f"\nadjustments applied:\n")
                f.write(f"  mean: {adjustments.mean():.1f} ms\n")
                f.write(f"  range: {adjustments.min():.1f} - {adjustments.max():.1f} ms\n")
        
        print(f"saved report to: {report_path}")

def apply_reaction_time_adjustments(gaze_csv: str, users_csv: str, output_csv: str = None):
    """
    main function to apply reaction time adjustments
    
    Args:
        gaze_csv: path to screen&gaze_scaled_last15s.csv
        users_csv: path to users_data_raw.csv
        output_csv: optional output path
    
    Returns:
        adjusted dataframe
    """
    # initialize adjuster
    adjuster = ReactionTimeAdjuster(gaze_csv, users_csv)
    
    # process dataset
    adjusted_df = adjuster.process_dataset()
    
    # save results
    adjuster.save_adjusted_data(adjusted_df, output_csv)
    
    print("\n" + "="*50)
    print("reaction time adjustment complete!")
    print("="*50)
    print("\nnext steps:")
    print("1. use the adjusted csv for hazard dataset creation")
    print("2. re-run the feature extraction pipeline")
    print("3. proceed with yolo integration")
    
    return adjusted_df

# unified pipeline usage
if __name__ == "__main__":
    print("="*80)
    print("reaction time preprocessing - unified pipeline")
    print("="*80)
    
    # import unified preprocessing utilities
    from preprocessing_utils import get_preprocessing_utils
    utils = get_preprocessing_utils()
    
    # load output from previous step (render_delay)
    gaze_df = utils.load_previous_output('render_delay', 'results')
    if gaze_df.empty:
        print("error: no gaze data found from previous step")
        exit(1)
    
    # load user demographics data
    users_df = utils.download_raw_data('users')
    if users_df.empty:
        print("warning: no user demographics data found, skipping adjustments")
        processed_df = gaze_df
    else:
        # create temporary files for processing
        temp_gaze = "/tmp/temp_gaze.csv"
        temp_users = "/tmp/temp_users.csv"
        temp_output = "/tmp/temp_output.csv"
        
        gaze_df.to_csv(temp_gaze, index=False)
        users_df.to_csv(temp_users, index=False)
        
        # apply reaction time adjustments
        processed_df = apply_reaction_time_adjustments(temp_gaze, temp_users, temp_output)
    
    print(f"processed {len(processed_df)} records")
    
    # save to unified S3 structure
    saved_keys = utils.save_unified_output(
        processed_df, 
        data_type='results',
        script_name='reaction_time'
    )
    
    print(f"\n" + "="*80)
    print("reaction time preprocessing complete!")
    print("="*80)
    print(f"output files saved to S3:")
    for key in saved_keys[:3]:
        print(f"  - s3://{utils.bucket}/{key}")
    if len(saved_keys) > 3:
        print(f"  - ... and {len(saved_keys) - 3} more files")
    
    print(f"\nadjusted data ready for structure processing!")