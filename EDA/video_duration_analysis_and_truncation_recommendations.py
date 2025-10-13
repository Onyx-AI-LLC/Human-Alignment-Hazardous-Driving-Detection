"""
Analyzes video simulation durations and generates code for truncating videos exceeding 15 seconds.
Examines gaze sampling rates, duration distributions, and their impact on hazard detection performance.
Creates comprehensive visualizations and provides automated truncation recommendations with generated code.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
from scipy import stats
from typing import Dict, List, Tuple
import warnings
import os

warnings.filterwarnings('ignore')

# set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

class VideoLengthAnalyzer:
    def __init__(self, scaled_df_path: str):
        """
        initialize analyzer with scaled gaze data
        
        Args:
            scaled_df_path: path to screen&gaze_scaled.csv
        """
        print("loading scaled gaze data...")
        self.scaled_df = pd.read_csv(scaled_df_path)
        print(f"loaded {len(self.scaled_df)} records")
        
    def analyze_video_lengths(self):
        """
        analyze the length of each video simulation
        """
        print("\n" + "="*50)
        print("video length analysis")
        print("="*50)
        
        video_lengths = []
        
        for idx, row in self.scaled_df.iterrows():
            # get session duration from the data
            session_duration = row.get('session_duration', 0)
            
            # calculate from timestamps
            try:
                coords = json.loads(row['all_transformed_coords'])
                if coords:
                    timestamps = [c['time'] for c in coords if 'time' in c]
                    if timestamps:
                        # calculate duration in seconds
                        calc_duration = (max(timestamps) - min(timestamps)) / 1000
                        
                        # also get first and last timestamp for absolute timing
                        first_timestamp = min(timestamps)
                        last_timestamp = max(timestamps)
                        
                        video_lengths.append({
                            'record_id': row['record_id'],
                            'user_id': row['user_id'],
                            'video_id': row['video_id'],
                            'reported_duration': session_duration,
                            'calculated_duration': calc_duration,
                            'first_timestamp': first_timestamp,
                            'last_timestamp': last_timestamp,
                            'num_gaze_points': len(timestamps),
                            'sampling_rate': len(timestamps) / calc_duration if calc_duration > 0 else 0,
                            'hazard_detected': row.get('hazard_detected', False),
                            'num_spacebar_presses': row.get('num_spacebar_presses', 0)
                        })
            except Exception as e:
                print(f"error processing record {idx}: {e}")
                continue
        
        length_df = pd.DataFrame(video_lengths)
        
        if len(length_df) == 0:
            print("no valid duration data found")
            return None
        
        # create comprehensive visualization
        fig, axes = plt.subplots(3, 3, figsize=(15, 12))
        fig.suptitle('Video Simulation Duration Analysis', fontsize=16)
        
        # 1. distribution of video lengths
        ax = axes[0, 0]
        durations = length_df['calculated_duration'].dropna()
        ax.hist(durations, bins=50, edgecolor='black', alpha=0.7, color='steelblue')
        ax.axvline(15, color='red', linestyle='--', linewidth=2, label='15 second target')
        ax.set_xlabel('Duration (seconds)')
        ax.set_ylabel('Count')
        ax.set_title('Distribution of Video Lengths')
        ax.legend()
        
        # add statistics
        ax.text(0.02, 0.98, f'Mean: {durations.mean():.1f}s\n'
                           f'Median: {durations.median():.1f}s\n'
                           f'Std: {durations.std():.1f}s\n'
                           f'Min: {durations.min():.1f}s\n'
                           f'Max: {durations.max():.1f}s',
                transform=ax.transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        # 2. videos over/under 15 seconds
        ax = axes[0, 1]
        over_15 = (durations > 15).sum()
        under_15 = (durations <= 15).sum()
        colors = ['green', 'orange']
        wedges, texts, autotexts = ax.pie([under_15, over_15], 
                                          labels=['≤15 seconds', '>15 seconds'],
                                          colors=colors,
                                          autopct='%1.1f%%',
                                          startangle=90)
        ax.set_title('Videos Meeting 15-Second Criterion')
        
        # add count in center
        centre_circle = plt.Circle((0, 0), 0.70, fc='white')
        ax.add_artist(centre_circle)
        ax.text(0, 0, f'Total\n{len(durations)}', ha='center', va='center', fontsize=12)
        
        # 3. duration by unique video_id
        ax = axes[0, 2]
        video_stats = length_df.groupby('video_id')['calculated_duration'].agg(['mean', 'std', 'count'])
        video_stats = video_stats.sort_values('mean', ascending=False).head(15)
        
        ax.barh(range(len(video_stats)), video_stats['mean'], xerr=video_stats['std'], 
               color='skyblue', edgecolor='navy', alpha=0.7)
        ax.set_yticks(range(len(video_stats)))
        ax.set_yticklabels([f'Video{vid}' for vid in video_stats.index], fontsize=8)
        ax.set_xlabel('Duration (seconds)')
        ax.set_title('Top 15 Videos by Mean Duration')
        ax.axvline(15, color='red', linestyle='--', alpha=0.5, label='15s target')
        
        # add sample counts
        for i, (idx, row) in enumerate(video_stats.iterrows()):
            ax.text(row['mean'] + row['std'] + 0.5, i, f"n={row['count']}", 
                   va='center', fontsize=7)
        
        # 4. gaze sampling rate distribution
        ax = axes[1, 0]
        sampling_rates = length_df['sampling_rate'].dropna()
        ax.hist(sampling_rates, bins=50, edgecolor='black', alpha=0.7, color='coral')
        ax.axvline(30, color='green', linestyle='--', linewidth=2, label='30 Hz (good)')
        ax.axvline(10, color='red', linestyle='--', linewidth=2, label='10 Hz (minimum)')
        ax.set_xlabel('Sampling Rate (Hz)')
        ax.set_ylabel('Count')
        ax.set_title('Gaze Sampling Rate Distribution')
        ax.legend()
        
        # 5. duration vs number of gaze points
        ax = axes[1, 1]
        ax.scatter(length_df['calculated_duration'], length_df['num_gaze_points'], 
                  alpha=0.3, s=10, c='darkblue')
        ax.set_xlabel('Duration (seconds)')
        ax.set_ylabel('Number of Gaze Points')
        ax.set_title('Gaze Points vs Duration')
        
        # fit linear regression
        mask = length_df['calculated_duration'].notna() & length_df['num_gaze_points'].notna()
        if mask.sum() > 0:
            z = np.polyfit(length_df.loc[mask, 'calculated_duration'], 
                          length_df.loc[mask, 'num_gaze_points'], 1)
            p = np.poly1d(z)
            x_line = np.linspace(length_df['calculated_duration'].min(), 
                                length_df['calculated_duration'].max(), 100)
            ax.plot(x_line, p(x_line), "r-", alpha=0.5, 
                   label=f'Rate: {z[0]:.1f} points/sec')
            ax.legend()
        
        # 6. reported vs calculated duration
        ax = axes[1, 2]
        if 'reported_duration' in length_df.columns:
            mask = (length_df['reported_duration'] > 0) & (length_df['calculated_duration'] > 0)
            if mask.sum() > 0:
                ax.scatter(length_df.loc[mask, 'reported_duration'], 
                          length_df.loc[mask, 'calculated_duration'], 
                          alpha=0.3, s=10, c='purple')
                
                # add perfect agreement line
                max_val = max(length_df.loc[mask, 'reported_duration'].max(),
                            length_df.loc[mask, 'calculated_duration'].max())
                ax.plot([0, max_val], [0, max_val], 'r--', alpha=0.5, label='Perfect agreement')
                
                # calculate correlation
                corr = length_df.loc[mask, 'reported_duration'].corr(
                    length_df.loc[mask, 'calculated_duration'])
                ax.text(0.05, 0.95, f'Correlation: {corr:.3f}', 
                       transform=ax.transAxes)
                
                ax.set_xlabel('Reported Duration (seconds)')
                ax.set_ylabel('Calculated Duration (seconds)')
                ax.set_title('Reported vs Calculated Duration')
                ax.legend()
        
        # 7. duration distribution by user
        ax = axes[2, 0]
        user_stats = length_df.groupby('user_id')['calculated_duration'].agg(['mean', 'count'])
        ax.hist(user_stats['mean'], bins=30, edgecolor='black', alpha=0.7, color='green')
        ax.axvline(15, color='red', linestyle='--', linewidth=2, label='15 second target')
        ax.set_xlabel('Mean Duration per User (seconds)')
        ax.set_ylabel('Number of Users')
        ax.set_title('User-Level Duration Patterns')
        ax.legend()
        
        # 8. videos requiring truncation
        ax = axes[2, 1]
        truncation_bins = [0, 5, 10, 15, 20, 25, 30, 100]
        truncation_labels = ['0-5s', '5-10s', '10-15s', '15-20s', '20-25s', '25-30s', '>30s']
        length_df['duration_bin'] = pd.cut(length_df['calculated_duration'], 
                                          bins=truncation_bins, 
                                          labels=truncation_labels)
        bin_counts = length_df['duration_bin'].value_counts()
        
        colors_map = {'0-5s': 'red', '5-10s': 'orange', '10-15s': 'yellow', 
                     '15-20s': 'lightcoral', '20-25s': 'salmon', 
                     '25-30s': 'darkred', '>30s': 'maroon'}
        bar_colors = [colors_map.get(x, 'gray') for x in bin_counts.index]
        
        ax.bar(range(len(bin_counts)), bin_counts.values, color=bar_colors, edgecolor='black')
        ax.set_xticks(range(len(bin_counts)))
        ax.set_xticklabels(bin_counts.index, rotation=45)
        ax.set_ylabel('Number of Videos')
        ax.set_title('Videos by Duration Range')
        ax.axhline(y=0, color='black', linewidth=0.5)
        
        # add percentage labels
        for i, v in enumerate(bin_counts.values):
            ax.text(i, v + 1, f'{v/len(length_df)*100:.1f}%', ha='center', fontsize=9)
        
        # 9. impact on hazard detection
        ax = axes[2, 2]
        if 'hazard_detected' in length_df.columns:
            # group by duration bins and calculate hazard detection rate
            hazard_by_duration = length_df.groupby('duration_bin')['hazard_detected'].mean()
            
            ax.bar(range(len(hazard_by_duration)), hazard_by_duration.values, 
                  color='darkblue', alpha=0.7, edgecolor='black')
            ax.set_xticks(range(len(hazard_by_duration)))
            ax.set_xticklabels(hazard_by_duration.index, rotation=45)
            ax.set_ylabel('Hazard Detection Rate')
            ax.set_title('Hazard Detection by Video Duration')
            ax.set_ylim(0, 1)
            
            # add horizontal line for overall rate
            overall_rate = length_df['hazard_detected'].mean()
            ax.axhline(overall_rate, color='red', linestyle='--', 
                      label=f'Overall: {overall_rate:.2f}')
            ax.legend()
        
        plt.tight_layout()
        
        # Save to eda_figures directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        figures_dir = os.path.join(script_dir, 'eda_figures')
        os.makedirs(figures_dir, exist_ok=True)
        plt.savefig(os.path.join(figures_dir, 'video_duration_analysis_comprehensive.png'), dpi=300, bbox_inches='tight')
        plt.show()
        
        # print detailed statistics
        self._print_statistics(length_df, durations)
        
        return length_df
    
    def _print_statistics(self, length_df: pd.DataFrame, durations: pd.Series):
        """
        print detailed statistics about video lengths
        """
        print("\nvideo length summary:")
        print("-" * 50)
        print(f"total videos analyzed: {len(length_df)}")
        print(f"unique video IDs: {length_df['video_id'].nunique()}")
        print(f"unique users: {length_df['user_id'].nunique()}")
        
        print("\nduration statistics:")
        print(f"  mean: {durations.mean():.2f} seconds")
        print(f"  median: {durations.median():.2f} seconds")
        print(f"  std: {durations.std():.2f} seconds")
        print(f"  min: {durations.min():.2f} seconds")
        print(f"  max: {durations.max():.2f} seconds")
        
        # compliance with 15-second limit
        under_15 = (durations <= 15).sum()
        over_15 = (durations > 15).sum()
        print(f"\n15-second compliance:")
        print(f"  ≤15 seconds: {under_15} ({under_15/len(durations)*100:.1f}%)")
        print(f"  >15 seconds: {over_15} ({over_15/len(durations)*100:.1f}%)")
        
        if over_15 > 0:
            over_15_df = length_df[length_df['calculated_duration'] > 15]
            print(f"\n  videos requiring truncation:")
            print(f"    15-20s: {((over_15_df['calculated_duration'] <= 20).sum())}")
            print(f"    20-25s: {((over_15_df['calculated_duration'] > 20) & (over_15_df['calculated_duration'] <= 25)).sum()}")
            print(f"    25-30s: {((over_15_df['calculated_duration'] > 25) & (over_15_df['calculated_duration'] <= 30)).sum()}")
            print(f"    >30s: {(over_15_df['calculated_duration'] > 30).sum()}")
        
        # sampling rate analysis
        print(f"\nsampling rate statistics:")
        print(f"  mean: {length_df['sampling_rate'].mean():.1f} Hz")
        print(f"  median: {length_df['sampling_rate'].median():.1f} Hz")
        print(f"  min: {length_df['sampling_rate'].min():.1f} Hz")
        print(f"  max: {length_df['sampling_rate'].max():.1f} Hz")
        
        low_sampling = (length_df['sampling_rate'] < 10).sum()
        if low_sampling > 0:
            print(f"  WARNING: {low_sampling} videos have sampling rate <10 Hz")
        
        # videos that are too short
        very_short = (durations < 5).sum()
        if very_short > 0:
            print(f"\nWARNING: {very_short} videos are shorter than 5 seconds")
            print("   these may have insufficient data for analysis")
    
    def create_truncation_code(self, length_df: pd.DataFrame):
        """
        generate code to truncate videos to 15 seconds
        """
        print("\n" + "="*50)
        print("video truncation code")
        print("="*50)
        
        videos_to_truncate = length_df[length_df['calculated_duration'] > 15]
        
        if len(videos_to_truncate) == 0:
            print("no videos need truncation!")
            return None
        
        print(f"need to truncate {len(videos_to_truncate)} videos")
        print("\ncode to truncate videos to 15 seconds:")
        print("-" * 40)
        
        truncation_code = '''
def truncate_videos_to_15_seconds(df, max_duration_ms=15000):
    """
    truncate all videos to maximum of 15 seconds
    
    Args:
        df: dataframe with screen&gaze_scaled data
        max_duration_ms: maximum duration in milliseconds (default 15000)
    
    Returns:
        dataframe with truncated videos
    """
    truncated_count = 0
    
    for idx, row in df.iterrows():
        try:
            # parse coordinates
            coords = json.loads(row['all_transformed_coords'])
            video_coords = json.loads(row['video_coords_only'])
            spacebar_times = json.loads(row['spacebar_timestamps'])
            
            if not coords:
                continue
            
            # get start time
            start_time = coords[0]['time']
            
            # truncate coordinates
            truncated_coords = [c for c in coords 
                              if c['time'] - start_time <= max_duration_ms]
            truncated_video = [c for c in video_coords 
                             if c['time'] - start_time <= max_duration_ms]
            truncated_spacebar = [t for t in spacebar_times 
                                if t - start_time <= max_duration_ms]
            
            # check if truncation occurred
            if len(truncated_coords) < len(coords):
                truncated_count += 1
                
                # update dataframe
                df.at[idx, 'all_transformed_coords'] = json.dumps(truncated_coords)
                df.at[idx, 'video_coords_only'] = json.dumps(truncated_video)
                df.at[idx, 'spacebar_timestamps'] = json.dumps(truncated_spacebar)
                
                # update counts
                df.at[idx, 'total_gaze_points'] = len(truncated_coords)
                df.at[idx, 'video_gaze_points'] = len(truncated_video)
                df.at[idx, 'num_spacebar_presses'] = len(truncated_spacebar)
                
                # recalculate session duration
                if truncated_coords:
                    new_duration = (truncated_coords[-1]['time'] - 
                                  truncated_coords[0]['time']) / 1000
                    df.at[idx, 'session_duration'] = new_duration
                    
        except Exception as e:
            print(f"error truncating record {idx}: {e}")
            continue
    
    print(f"truncated {truncated_count} videos to 15 seconds")
    return df

# apply truncation
df_truncated = truncate_videos_to_15_seconds(df.copy())
'''
        
        print(truncation_code)
        
        # Save truncation code to eda_data directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(script_dir, 'eda_data')
        os.makedirs(data_dir, exist_ok=True)
        
        code_path = os.path.join(data_dir, 'video_truncation_code.py')
        with open(code_path, 'w') as f:
            f.write("# Video Truncation Code Generated from eda Analysis\n")
            f.write("# This code truncates videos to 15 seconds maximum duration\n\n")
            f.write("import json\n")
            f.write("import pandas as pd\n\n")
            f.write(truncation_code.strip())
        
        print(f"\nSaved truncation code to: {code_path}")
        
        return truncation_code
    
    def save_analysis_results(self, length_df: pd.DataFrame, output_dir: str):
        """
        save analysis results to eda_data directory
        """
        if length_df is not None:
            # Save detailed analysis data
            analysis_path = os.path.join(output_dir, 'video_duration_analysis.csv')
            length_df.to_csv(analysis_path, index=False)
            print(f"\nsaved video duration analysis to: {analysis_path}")
            
            # Save summary statistics
            summary_path = os.path.join(output_dir, 'video_duration_summary.txt')
            with open(summary_path, 'w') as f:
                f.write("Video Duration Analysis Summary\n")
                f.write("="*50 + "\n\n")
                
                durations = length_df['calculated_duration']
                f.write(f"Total videos analyzed: {len(length_df)}\n")
                f.write(f"Unique video IDs: {length_df['video_id'].nunique()}\n")
                f.write(f"Unique users: {length_df['user_id'].nunique()}\n\n")
                
                f.write("Duration Statistics:\n")
                f.write(f"  Mean duration: {durations.mean():.2f} seconds\n")
                f.write(f"  Median duration: {durations.median():.2f} seconds\n")
                f.write(f"  Standard deviation: {durations.std():.2f} seconds\n")
                f.write(f"  Minimum duration: {durations.min():.2f} seconds\n")
                f.write(f"  Maximum duration: {durations.max():.2f} seconds\n\n")
                
                f.write("15-Second Compliance:\n")
                f.write(f"  Videos ≤15 seconds: {(durations <= 15).sum()} ({(durations <= 15).sum()/len(durations)*100:.1f}%)\n")
                f.write(f"  Videos >15 seconds: {(durations > 15).sum()} ({(durations > 15).sum()/len(durations)*100:.1f}%)\n\n")
                
                f.write("Quality Concerns:\n")
                f.write(f"  Videos <5 seconds: {(durations < 5).sum()} ({(durations < 5).sum()/len(durations)*100:.1f}%)\n")
                
                if length_df['sampling_rate'].notna().any():
                    f.write(f"\nSampling Rate Analysis:\n")
                    f.write(f"  Mean sampling rate: {length_df['sampling_rate'].mean():.1f} Hz\n")
                    f.write(f"  Videos with <10 Hz: {(length_df['sampling_rate'] < 10).sum()} videos\n")
            
            print(f"Saved summary to: {summary_path}")
            
            return analysis_path, summary_path

def run_video_duration_analysis(scaled_csv_path: str):
    """
    run complete video duration analysis
    
    Args:
        scaled_csv_path: path to screen&gaze_scaled.csv
    
    Returns:
        length_df: dataframe with video duration analysis
    """
    # Create output directories
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, 'eda_data')
    figures_dir = os.path.join(script_dir, 'eda_figures')
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(figures_dir, exist_ok=True)
    
    # initialize analyzer
    analyzer = VideoLengthAnalyzer(scaled_csv_path)
    
    # run analysis
    length_df = analyzer.analyze_video_lengths()
    
    if length_df is None:
        print("analysis failed - no valid data")
        return None
    
    # generate truncation code if needed
    truncation_code = analyzer.create_truncation_code(length_df)
    
    # save results to organized directories
    analysis_path, summary_path = analyzer.save_analysis_results(length_df, data_dir)
    
    # print recommendations
    print("\n" + "="*50)
    print("recommendations")
    print("="*50)
    
    if (length_df['calculated_duration'] > 15).any():
        print("1. Apply video truncation to 15 seconds using provided code")
    
    if (length_df['calculated_duration'] < 5).any():
        print("2. Consider removing videos shorter than 5 seconds")
    
    if (length_df['sampling_rate'] < 10).any():
        print("3. Filter out videos with sampling rate <10 Hz")
    
    print("4. After applying fixes, re-run the restructuring script")
    print("5. Then proceed with YOLO integration")
    
    print(f"\nFiles saved:")
    print(f"  - {analysis_path} (detailed analysis data)")
    print(f"  - {summary_path} (summary statistics)")
    print(f"  - {os.path.join(figures_dir, 'video_duration_analysis_comprehensive.png')} (visualizations)")
    if truncation_code:
        print(f"  - {os.path.join(data_dir, 'video_truncation_code.py')} (truncation code)")
    
    return length_df

# example usage
if __name__ == "__main__":
    # path to your scaled data
    scaled_csv = "../data/processed/screen&gaze_scaled.csv"
    
    # run analysis
    length_df = run_video_duration_analysis(scaled_csv)
    
    if length_df is not None:
        print("\n" + "="*50)
        print("analysis complete!")
        print("="*50)
        print("\nnext steps:")
        print("1. Review the video duration distribution")
        print("2. Apply truncation if needed using the generated code")
        print("3. Filter out problematic videos")
        print("4. Proceed with hazard detection pipeline")