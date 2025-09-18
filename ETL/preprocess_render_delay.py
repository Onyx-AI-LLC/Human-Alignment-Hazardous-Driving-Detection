import pandas as pd
import numpy as np
import json
import os
from typing import Dict, List, Tuple

class Last15SecondsExtractor:
    def __init__(self, input_path: str, duration_seconds: float = 15.0):
        """
        initialize extractor to get the last N seconds of each video
        
        Args:
            input_path: path to screen&gaze_scaled.csv
            duration_seconds: number of seconds to extract from the end (default 15)
        """
        self.input_path = input_path
        self.duration_ms = duration_seconds * 1000  # convert to milliseconds
        self.df = pd.read_csv(input_path)
        print(f"loaded {len(self.df)} records from {input_path}")
        
        # statistics tracking
        self.stats = {
            'total_records': len(self.df),
            'processed': 0,
            'truncated': 0,
            'too_short': 0,
            'errors': 0,
            'original_durations': [],
            'final_durations': []
        }
    
    def extract_last_seconds(self, gaze_coords: List[Dict], 
                            spacebar_times: List[float],
                            duration_ms: float) -> Tuple[List[Dict], List[float], float, float]:
        """
        extract the last N milliseconds of data
        
        Args:
            gaze_coords: list of gaze coordinate dictionaries
            spacebar_times: list of spacebar timestamps
            duration_ms: milliseconds to extract from the end
        
        Returns:
            tuple of (filtered_coords, filtered_spacebar, start_time, end_time)
        """
        if not gaze_coords:
            return [], [], 0, 0
        
        # get the last timestamp
        all_times = [c.get('time', 0) for c in gaze_coords]
        end_time = max(all_times)
        start_time = end_time - duration_ms
        
        # filter coordinates to last N seconds
        filtered_coords = []
        for coord in gaze_coords:
            if coord.get('time', 0) >= start_time:
                # keep original timestamp but also add relative time
                new_coord = coord.copy()
                new_coord['original_time'] = coord.get('time', 0)
                new_coord['relative_time'] = coord.get('time', 0) - start_time
                filtered_coords.append(new_coord)
        
        # filter spacebar timestamps
        filtered_spacebar = [t for t in spacebar_times if t >= start_time]
        
        return filtered_coords, filtered_spacebar, start_time, end_time
    
    def process_dataset(self) -> pd.DataFrame:
        """
        process all records to extract last 15 seconds
        """
        print("\n" + "="*50)
        print(f"extracting last {self.duration_ms/1000:.0f} seconds from each video")
        print("="*50)
        
        processed_records = []
        
        for idx, row in self.df.iterrows():
            if idx % 50 == 0:
                print(f"processing record {idx}/{len(self.df)}...")
            
            try:
                # parse json data
                all_coords = json.loads(row['all_transformed_coords'])
                video_coords = json.loads(row['video_coords_only'])
                spacebar_times = json.loads(row.get('spacebar_timestamps', '[]'))
                
                if not all_coords:
                    self.stats['errors'] += 1
                    continue
                
                # calculate original duration
                original_times = [c.get('time', 0) for c in all_coords]
                original_duration = (max(original_times) - min(original_times)) / 1000
                self.stats['original_durations'].append(original_duration)
                
                # check if video is long enough
                if original_duration * 1000 < self.duration_ms:
                    # video is shorter than target duration - keep as is
                    self.stats['too_short'] += 1
                    processed_record = row.copy()
                    self.stats['final_durations'].append(original_duration)
                else:
                    # extract last N seconds
                    filtered_all, filtered_spacebar, start_time, end_time = self.extract_last_seconds(
                        all_coords, spacebar_times, self.duration_ms
                    )
                    
                    # also filter video-only coordinates
                    filtered_video, _, _, _ = self.extract_last_seconds(
                        video_coords, [], self.duration_ms
                    )
                    
                    self.stats['truncated'] += 1
                    
                    # create processed record
                    processed_record = row.copy()
                    
                    # update with filtered data
                    processed_record['all_transformed_coords'] = json.dumps(filtered_all)
                    processed_record['video_coords_only'] = json.dumps(filtered_video)
                    processed_record['spacebar_timestamps'] = json.dumps(filtered_spacebar)
                    
                    # update counts and duration
                    processed_record['total_gaze_points'] = len(filtered_all)
                    processed_record['video_gaze_points'] = len(filtered_video)
                    processed_record['num_spacebar_presses'] = len(filtered_spacebar)
                    
                    # calculate new duration
                    if filtered_all:
                        new_duration = (end_time - start_time) / 1000
                        processed_record['session_duration'] = new_duration
                        self.stats['final_durations'].append(new_duration)
                    
                    # add metadata about extraction
                    processed_record['extraction_start_time'] = start_time
                    processed_record['extraction_end_time'] = end_time
                    processed_record['original_duration'] = original_duration
                
                processed_records.append(processed_record)
                self.stats['processed'] += 1
                
            except Exception as e:
                print(f"error processing record {idx}: {e}")
                self.stats['errors'] += 1
                continue
        
        # create dataframe
        processed_df = pd.DataFrame(processed_records)
        
        # print statistics
        self._print_statistics()
        
        return processed_df
    
    def _print_statistics(self):
        """
        print processing statistics
        """
        print("\n" + "="*50)
        print("processing statistics")
        print("="*50)
        
        print(f"total records: {self.stats['total_records']}")
        print(f"successfully processed: {self.stats['processed']}")
        print(f"videos truncated (from beginning): {self.stats['truncated']}")
        print(f"videos kept as-is (< {self.duration_ms/1000:.0f}s): {self.stats['too_short']}")
        print(f"processing errors: {self.stats['errors']}")
        
        if self.stats['original_durations']:
            orig_durations = np.array(self.stats['original_durations'])
            final_durations = np.array(self.stats['final_durations'])
            
            print("\noriginal durations:")
            print(f"  mean: {orig_durations.mean():.2f} seconds")
            print(f"  median: {np.median(orig_durations):.2f} seconds")
            print(f"  min: {orig_durations.min():.2f} seconds")
            print(f"  max: {orig_durations.max():.2f} seconds")
            
            print("\nfinal durations (after extraction):")
            print(f"  mean: {final_durations.mean():.2f} seconds")
            print(f"  median: {np.median(final_durations):.2f} seconds")
            print(f"  min: {final_durations.min():.2f} seconds")
            print(f"  max: {final_durations.max():.2f} seconds")
            
            # check how many are now within target
            within_target = np.sum(final_durations <= self.duration_ms/1000 + 0.1)
            print(f"\nvideos within {self.duration_ms/1000:.0f}s (±0.1s): {within_target} ({within_target/len(final_durations)*100:.1f}%)")
    
    def validate_extraction(self, processed_df: pd.DataFrame, sample_size: int = 5):
        """
        validate that extraction worked correctly on a sample
        """
        print("\n" + "="*50)
        print("validation check (sample)")
        print("="*50)
        
        # sample random records that were truncated
        truncated_records = processed_df[processed_df['original_duration'] > self.duration_ms/1000].head(sample_size)
        
        for idx, row in truncated_records.iterrows():
            print(f"\nrecord {row['record_id']}:")
            print(f"  original duration: {row.get('original_duration', 'N/A'):.2f}s")
            print(f"  new duration: {row['session_duration']:.2f}s")
            
            # check timestamps
            coords = json.loads(row['all_transformed_coords'])
            if coords:
                times = [c.get('time', c.get('original_time', 0)) for c in coords]
                actual_duration = (max(times) - min(times)) / 1000
                print(f"  verified duration: {actual_duration:.2f}s")
                
                # check spacebar retention
                spacebar = json.loads(row['spacebar_timestamps'])
                print(f"  spacebar presses retained: {len(spacebar)}")
    
    def save_processed_data(self, processed_df: pd.DataFrame, output_path: str = None):
        """
        save the processed dataframe
        """
        if output_path is None:
            # create output path based on input
            base_path = self.input_path.replace('.csv', '')
            output_path = f"{base_path}_last15s.csv"
        
        processed_df.to_csv(output_path, index=False)
        print(f"\nsaved processed data to: {output_path}")
        
        # also save a summary report
        report_path = output_path.replace('.csv', '_report.txt')
        with open(report_path, 'w') as f:
            f.write("last 15 seconds extraction report\n")
            f.write("="*50 + "\n\n")
            f.write(f"input file: {self.input_path}\n")
            f.write(f"output file: {output_path}\n")
            f.write(f"extraction duration: {self.duration_ms/1000:.0f} seconds\n\n")
            
            f.write("processing statistics:\n")
            f.write(f"  total records: {self.stats['total_records']}\n")
            f.write(f"  processed: {self.stats['processed']}\n")
            f.write(f"  truncated: {self.stats['truncated']}\n")
            f.write(f"  kept as-is: {self.stats['too_short']}\n")
            f.write(f"  errors: {self.stats['errors']}\n\n")
            
            if self.stats['final_durations']:
                final_durations = np.array(self.stats['final_durations'])
                f.write("final duration statistics:\n")
                f.write(f"  mean: {final_durations.mean():.2f} seconds\n")
                f.write(f"  std: {final_durations.std():.2f} seconds\n")
                f.write(f"  min: {final_durations.min():.2f} seconds\n")
                f.write(f"  max: {final_durations.max():.2f} seconds\n")
        
        print(f"saved report to: {report_path}")

def extract_last_15_seconds(input_path: str, output_path: str = None, duration_seconds: float = 15.0):
    """
    main function to extract last N seconds from each video
    
    Args:
        input_path: path to screen&gaze_scaled.csv
        output_path: optional output path (auto-generated if None)
        duration_seconds: seconds to extract from end (default 15)
    
    Returns:
        processed dataframe
    """
    # initialize extractor
    extractor = Last15SecondsExtractor(input_path, duration_seconds)
    
    # process dataset
    processed_df = extractor.process_dataset()
    
    # validate on sample
    extractor.validate_extraction(processed_df)
    
    # save results
    extractor.save_processed_data(processed_df, output_path)
    
    print("\n" + "="*50)
    print("extraction complete!")
    print("="*50)
    print("\nnext steps:")
    print("1. use the new '_last15s.csv' file for further processing")
    print("2. re-run the hazard training dataset creation")
    print("3. proceed with yolo integration")
    
    return processed_df

# example usage
if __name__ == "__main__":
    # input and output paths
    input_csv = "data/processed/screen&gaze_scaled.csv"
    output_csv = "data/processed/screen&gaze_scaled_last15s.csv"
    
    # extract last 15 seconds from each video
    processed_df = extract_last_15_seconds(input_csv, output_csv, duration_seconds=15.0)
    
    print(f"\nprocessed {len(processed_df)} records")
    print(f"data ready for hazard detection pipeline")