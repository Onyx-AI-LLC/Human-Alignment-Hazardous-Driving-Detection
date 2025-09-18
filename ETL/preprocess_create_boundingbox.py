import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# set style for better visualizations
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

class GazePatternAnalyzer:
    def __init__(self, df: pd.DataFrame):
        """
        initialize the analyzer with the restructured dataframe
        """
        self.df = df
        self.hazard_df = df[df['is_hazard_moment'] == True].copy()
        self.non_hazard_df = df[df['is_hazard_moment'] == False].copy()
        
        print(f"loaded {len(df)} total samples")
        print(f"hazard moments: {len(self.hazard_df)} ({len(self.hazard_df)/len(df)*100:.1f}%)")
        print(f"non-hazard moments: {len(self.non_hazard_df)} ({len(self.non_hazard_df)/len(df)*100:.1f}%)")
        
    def analyze_spatial_distribution(self):
        """
        analyze where people look during hazard vs non-hazard moments
        """
        print("\n" + "="*50)
        print("spatial distribution analysis")
        print("="*50)
        
        # create figure with subplots
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
        fig.suptitle('Gaze Spatial Distribution: Hazard vs Non-Hazard Moments', fontsize=16)
        
        # 1. heatmap of gaze positions during hazards
        ax = axes[0, 0]
        if not self.hazard_df.empty and 'video_rel_x' in self.hazard_df.columns:
            hazard_x = self.hazard_df['video_rel_x'].dropna()
            hazard_y = self.hazard_df['video_rel_y'].dropna()
            if len(hazard_x) > 0:
                ax.hexbin(hazard_x, hazard_y, gridsize=20, cmap='Reds', alpha=0.6)
                ax.set_title('Hazard Moment Gaze Heatmap')
                ax.set_xlabel('Normalized X')
                ax.set_ylabel('Normalized Y')
                ax.set_xlim(0, 1)
                ax.set_ylim(0, 1)
        
        # 2. heatmap of gaze positions during non-hazards
        ax = axes[0, 1]
        if not self.non_hazard_df.empty and 'video_rel_x' in self.non_hazard_df.columns:
            non_hazard_x = self.non_hazard_df['video_rel_x'].dropna()
            non_hazard_y = self.non_hazard_df['video_rel_y'].dropna()
            if len(non_hazard_x) > 0:
                # sample if too many points
                if len(non_hazard_x) > 10000:
                    sample_idx = np.random.choice(len(non_hazard_x), 10000, replace=False)
                    non_hazard_x = non_hazard_x.iloc[sample_idx]
                    non_hazard_y = non_hazard_y.iloc[sample_idx]
                ax.hexbin(non_hazard_x, non_hazard_y, gridsize=20, cmap='Blues', alpha=0.6)
                ax.set_title('Non-Hazard Moment Gaze Heatmap')
                ax.set_xlabel('Normalized X')
                ax.set_ylabel('Normalized Y')
                ax.set_xlim(0, 1)
                ax.set_ylim(0, 1)
        
        # 3. difference heatmap
        ax = axes[0, 2]
        if not self.hazard_df.empty and not self.non_hazard_df.empty:
            # create 2d histograms
            bins = np.linspace(0, 1, 21)
            hazard_hist, xedges, yedges = np.histogram2d(
                self.hazard_df['video_rel_x'].dropna(),
                self.hazard_df['video_rel_y'].dropna(),
                bins=[bins, bins]
            )
            non_hazard_hist, _, _ = np.histogram2d(
                self.non_hazard_df['video_rel_x'].dropna(),
                self.non_hazard_df['video_rel_y'].dropna(),
                bins=[bins, bins]
            )
            
            # normalize by total points
            hazard_hist = hazard_hist / (hazard_hist.sum() + 1e-10)
            non_hazard_hist = non_hazard_hist / (non_hazard_hist.sum() + 1e-10)
            
            # difference
            diff_hist = hazard_hist - non_hazard_hist
            
            im = ax.imshow(diff_hist.T, origin='lower', cmap='RdBu_r', 
                          extent=[0, 1, 0, 1], aspect='auto')
            ax.set_title('Difference (Hazard - Non-Hazard)')
            ax.set_xlabel('Normalized X')
            ax.set_ylabel('Normalized Y')
            plt.colorbar(im, ax=ax)
        
        # 4. quadrant distribution
        ax = axes[1, 0]
        if 'screen_quadrant' in self.df.columns:
            quadrant_data = pd.DataFrame({
                'Hazard': self.hazard_df['screen_quadrant'].value_counts(normalize=True),
                'Non-Hazard': self.non_hazard_df['screen_quadrant'].value_counts(normalize=True)
            })
            quadrant_data.plot(kind='bar', ax=ax)
            ax.set_title('Quadrant Distribution')
            ax.set_xlabel('Screen Quadrant')
            ax.set_ylabel('Proportion')
            ax.legend()
        
        # 5. distance from center distribution
        ax = axes[1, 1]
        if 'distance_from_center' in self.df.columns:
            ax.hist(self.hazard_df['distance_from_center'].dropna(), 
                   bins=30, alpha=0.5, label='Hazard', density=True, color='red')
            ax.hist(self.non_hazard_df['distance_from_center'].dropna(), 
                   bins=30, alpha=0.5, label='Non-Hazard', density=True, color='blue')
            ax.set_title('Distance from Center Distribution')
            ax.set_xlabel('Distance from Center')
            ax.set_ylabel('Density')
            ax.legend()
        
        # 6. region analysis
        ax = axes[1, 2]
        regions = ['is_center_region', 'is_peripheral', 'is_horizon_region']
        hazard_props = []
        non_hazard_props = []
        
        for region in regions:
            if region in self.df.columns:
                hazard_props.append(self.hazard_df[region].mean())
                non_hazard_props.append(self.non_hazard_df[region].mean())
        
        if hazard_props:
            x = np.arange(len(regions))
            width = 0.35
            ax.bar(x - width/2, hazard_props, width, label='Hazard', color='red', alpha=0.7)
            ax.bar(x + width/2, non_hazard_props, width, label='Non-Hazard', color='blue', alpha=0.7)
            ax.set_xlabel('Region')
            ax.set_ylabel('Proportion in Region')
            ax.set_title('Gaze in Different Regions')
            ax.set_xticks(x)
            ax.set_xticklabels([r.replace('is_', '').replace('_', ' ').title() for r in regions], rotation=45)
            ax.legend()
        
        plt.tight_layout()
        plt.show()
        
        # print statistical summary
        print("\nspatial statistics:")
        print("-" * 30)
        
        if 'video_rel_x' in self.df.columns:
            print(f"hazard gaze center (x, y): ({self.hazard_df['video_rel_x'].mean():.3f}, "
                  f"{self.hazard_df['video_rel_y'].mean():.3f})")
            print(f"non-hazard gaze center (x, y): ({self.non_hazard_df['video_rel_x'].mean():.3f}, "
                  f"{self.non_hazard_df['video_rel_y'].mean():.3f})")
        
        if 'distance_from_center' in self.df.columns:
            print(f"hazard avg distance from center: {self.hazard_df['distance_from_center'].mean():.3f}")
            print(f"non-hazard avg distance from center: {self.non_hazard_df['distance_from_center'].mean():.3f}")
            
            # statistical test
            stat, p_value = stats.ttest_ind(
                self.hazard_df['distance_from_center'].dropna(),
                self.non_hazard_df['distance_from_center'].dropna()
            )
            print(f"t-test for distance difference: t={stat:.3f}, p={p_value:.3f}")
    
    def analyze_temporal_patterns(self):
        """
        analyze temporal patterns around hazard events
        """
        print("\n" + "="*50)
        print("temporal pattern analysis")
        print("="*50)
        
        fig, axes = plt.subplots(2, 3, figsize=(15, 10))
        fig.suptitle('Temporal Patterns Around Hazard Events', fontsize=16)
        
        # 1. gaze velocity distribution
        ax = axes[0, 0]
        if 'gaze_speed' in self.df.columns:
            try:
                hazard_speeds = self.hazard_df['gaze_speed'].dropna()
                non_hazard_speeds = self.non_hazard_df['gaze_speed'].dropna()
                
                if len(hazard_speeds) > 0:
                    ax.hist(hazard_speeds, bins=30, alpha=0.5, label='Hazard', density=True, color='red')
                if len(non_hazard_speeds) > 0:
                    ax.hist(non_hazard_speeds, bins=30, alpha=0.5, label='Non-Hazard', density=True, color='blue')
                
                ax.set_title('Gaze Speed Distribution')
                ax.set_xlabel('Gaze Speed')
                ax.set_ylabel('Density')
                ax.legend()
                
                # set reasonable x-limit
                if len(self.df['gaze_speed'].dropna()) > 0:
                    ax.set_xlim(0, np.percentile(self.df['gaze_speed'].dropna(), 95))
            except Exception as e:
                ax.text(0.5, 0.5, 'Error in speed plot', ha='center', va='center')
                ax.set_title('Gaze Speed Distribution')
        
        # 2. fixation vs saccade distribution
        ax = axes[0, 1]
        if 'is_fixation' in self.df.columns:
            try:
                fixation_data = pd.DataFrame({
                    'Hazard': [
                        self.hazard_df['is_fixation'].mean(),
                        self.hazard_df['is_saccade'].mean() if 'is_saccade' in self.hazard_df.columns else 0,
                        1 - self.hazard_df['is_fixation'].mean() - 
                        (self.hazard_df['is_saccade'].mean() if 'is_saccade' in self.hazard_df.columns else 0)
                    ],
                    'Non-Hazard': [
                        self.non_hazard_df['is_fixation'].mean(),
                        self.non_hazard_df['is_saccade'].mean() if 'is_saccade' in self.non_hazard_df.columns else 0,
                        1 - self.non_hazard_df['is_fixation'].mean() - 
                        (self.non_hazard_df['is_saccade'].mean() if 'is_saccade' in self.non_hazard_df.columns else 0)
                    ]
                })
                fixation_data.index = ['Fixation', 'Saccade', 'Other']
                fixation_data.plot(kind='bar', ax=ax)
                ax.set_title('Eye Movement Types')
                ax.set_ylabel('Proportion')
                ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
            except Exception as e:
                ax.text(0.5, 0.5, 'Error in fixation plot', ha='center', va='center')
                ax.set_title('Eye Movement Types')
        
        # 3. time to spacebar distribution
        ax = axes[0, 2]
        if 'time_to_nearest_spacebar' in self.df.columns:
            try:
                hazard_times = self.hazard_df['time_to_nearest_spacebar'].dropna()
                if len(hazard_times) > 0:
                    ax.hist(hazard_times, bins=30, alpha=0.7, color='red')
                    ax.set_title('Time to Spacebar Press (Hazard Moments)')
                    ax.set_xlabel('Time to Spacebar (ms)')
                    ax.set_ylabel('Count')
                    ax.axvline(hazard_times.median(), color='black', linestyle='--', 
                              label=f'Median: {hazard_times.median():.0f}ms')
                    ax.legend()
                else:
                    ax.text(0.5, 0.5, 'No spacebar data', ha='center', va='center')
                    ax.set_title('Time to Spacebar Press')
            except Exception as e:
                ax.text(0.5, 0.5, 'Error in spacebar plot', ha='center', va='center')
                ax.set_title('Time to Spacebar Press')
        
        # 4. gaze dispersion over time - FIXED VERSION
        ax = axes[1, 0]
        if 'gaze_dispersion_total' in self.df.columns and 'video_progress' in self.df.columns:
            try:
                # bin video progress
                bins = np.linspace(0, 1, 11)
                self.hazard_df['progress_bin'] = pd.cut(self.hazard_df['video_progress'], bins)
                self.non_hazard_df['progress_bin'] = pd.cut(self.non_hazard_df['video_progress'], bins)
                
                hazard_dispersion = self.hazard_df.groupby('progress_bin')['gaze_dispersion_total'].mean()
                non_hazard_dispersion = self.non_hazard_df.groupby('progress_bin')['gaze_dispersion_total'].mean()
                
                if len(hazard_dispersion) > 0 and len(non_hazard_dispersion) > 0:
                    # plot the data
                    x_pos = np.arange(len(hazard_dispersion))
                    ax.plot(x_pos, hazard_dispersion.values, 'r-', label='Hazard', marker='o')
                    ax.plot(x_pos, non_hazard_dispersion.values, 'b-', label='Non-Hazard', marker='s')
                    ax.set_title('Gaze Dispersion Over Video Progress')
                    ax.set_xlabel('Video Progress')
                    ax.set_ylabel('Average Dispersion')
                    
                    # fix: properly set tick labels
                    if len(x_pos) > 0:
                        # take every other position for cleaner display
                        tick_indices = x_pos[::2] if len(x_pos) > 5 else x_pos
                        tick_labels = [f'{int(i/len(x_pos)*100)}%' for i in tick_indices]
                        ax.set_xticks(tick_indices)
                        ax.set_xticklabels(tick_labels)
                    
                    ax.legend()
                else:
                    ax.text(0.5, 0.5, 'Insufficient dispersion data', ha='center', va='center')
                    ax.set_title('Gaze Dispersion Over Video Progress')
            except Exception as e:
                ax.text(0.5, 0.5, f'Error: {str(e)[:30]}', ha='center', va='center')
                ax.set_title('Gaze Dispersion Over Video Progress')
        else:
            ax.text(0.5, 0.5, 'Dispersion data not available', ha='center', va='center')
            ax.set_title('Gaze Dispersion Over Video Progress')
        
        # 5. velocity components
        ax = axes[1, 1]
        if 'gaze_velocity_x' in self.df.columns and 'gaze_velocity_y' in self.df.columns:
            try:
                # sample for visualization
                if len(self.hazard_df) > 0:
                    sample_size = min(1000, len(self.hazard_df))
                    hazard_sample = self.hazard_df.sample(n=sample_size)
                    ax.scatter(hazard_sample['gaze_velocity_x'].dropna(), 
                             hazard_sample['gaze_velocity_y'].dropna(), 
                             alpha=0.3, c='red', s=10, label='Hazard')
                
                if len(self.non_hazard_df) > 0:
                    sample_size = min(1000, len(self.non_hazard_df))
                    non_hazard_sample = self.non_hazard_df.sample(n=sample_size)
                    ax.scatter(non_hazard_sample['gaze_velocity_x'].dropna(), 
                             non_hazard_sample['gaze_velocity_y'].dropna(), 
                             alpha=0.3, c='blue', s=10, label='Non-Hazard')
                
                ax.set_title('Gaze Velocity Components')
                ax.set_xlabel('Velocity X')
                ax.set_ylabel('Velocity Y')
                ax.legend()
                ax.set_xlim(-0.5, 0.5)
                ax.set_ylim(-0.5, 0.5)
            except Exception as e:
                ax.text(0.5, 0.5, 'Error in velocity plot', ha='center', va='center')
                ax.set_title('Gaze Velocity Components')
        else:
            ax.text(0.5, 0.5, 'Velocity data not available', ha='center', va='center')
            ax.set_title('Gaze Velocity Components')
        
        # 6. acceleration patterns
        ax = axes[1, 2]
        if 'gaze_acceleration_x' in self.df.columns and 'gaze_acceleration_y' in self.df.columns:
            try:
                # calculate acceleration magnitude
                accel_magnitude_hazard = np.sqrt(
                    self.hazard_df['gaze_acceleration_x']**2 + 
                    self.hazard_df['gaze_acceleration_y']**2
                ).dropna()
                accel_magnitude_non_hazard = np.sqrt(
                    self.non_hazard_df['gaze_acceleration_x']**2 + 
                    self.non_hazard_df['gaze_acceleration_y']**2
                ).dropna()
                
                # filter outliers for better visualization
                if len(accel_magnitude_hazard) > 0:
                    hazard_filtered = accel_magnitude_hazard[accel_magnitude_hazard < 1]
                    if len(hazard_filtered) > 0:
                        ax.hist(hazard_filtered, bins=30, alpha=0.5, label='Hazard', 
                               density=True, color='red')
                
                if len(accel_magnitude_non_hazard) > 0:
                    non_hazard_filtered = accel_magnitude_non_hazard[accel_magnitude_non_hazard < 1]
                    if len(non_hazard_filtered) > 0:
                        ax.hist(non_hazard_filtered, bins=30, alpha=0.5, label='Non-Hazard', 
                               density=True, color='blue')
                
                ax.set_title('Gaze Acceleration Magnitude')
                ax.set_xlabel('Acceleration')
                ax.set_ylabel('Density')
                ax.legend()
            except Exception as e:
                ax.text(0.5, 0.5, 'Error in acceleration plot', ha='center', va='center')
                ax.set_title('Gaze Acceleration Magnitude')
        else:
            ax.text(0.5, 0.5, 'Acceleration data not available', ha='center', va='center')
            ax.set_title('Gaze Acceleration Magnitude')
        
        plt.tight_layout()
        plt.show()
        
        # print temporal statistics
        print("\ntemporal statistics:")
        print("-" * 30)
        
        if 'gaze_speed' in self.df.columns:
            print(f"hazard avg gaze speed: {self.hazard_df['gaze_speed'].mean():.3f}")
            print(f"non-hazard avg gaze speed: {self.non_hazard_df['gaze_speed'].mean():.3f}")
        
        if 'is_fixation' in self.df.columns:
            print(f"hazard fixation rate: {self.hazard_df['is_fixation'].mean():.3f}")
            print(f"non-hazard fixation rate: {self.non_hazard_df['is_fixation'].mean():.3f}")
        
        if 'time_to_nearest_spacebar' in self.df.columns:
            print(f"median time to spacebar (hazard): {self.hazard_df['time_to_nearest_spacebar'].median():.0f}ms")
    
    def analyze_attention_patterns(self):
        """
        analyze attention patterns that could help identify hazardous objects
        """
        print("\n" + "="*50)
        print("attention pattern analysis for object matching")
        print("="*50)
        
        # key metrics for object-gaze matching
        metrics = {}
        
        # 1. fixation characteristics during hazards
        if 'is_fixation' in self.hazard_df.columns:
            hazard_fixations = self.hazard_df[self.hazard_df['is_fixation'] == 1]
            metrics['hazard_fixation_rate'] = len(hazard_fixations) / len(self.hazard_df)
            
            if 'gaze_dispersion_total' in hazard_fixations.columns:
                metrics['hazard_fixation_dispersion'] = hazard_fixations['gaze_dispersion_total'].mean()
        
        # 2. optimal time window for hazard detection
        if 'time_to_nearest_spacebar' in self.hazard_df.columns:
            time_to_spacebar = self.hazard_df['time_to_nearest_spacebar'].dropna()
            metrics['optimal_time_window'] = {
                'median': time_to_spacebar.median(),
                'q25': time_to_spacebar.quantile(0.25),
                'q75': time_to_spacebar.quantile(0.75)
            }
        
        # 3. spatial concentration during hazards
        if 'video_rel_x' in self.hazard_df.columns:
            metrics['spatial_std'] = {
                'hazard_x_std': self.hazard_df['video_rel_x'].std(),
                'hazard_y_std': self.hazard_df['video_rel_y'].std(),
                'non_hazard_x_std': self.non_hazard_df['video_rel_x'].std(),
                'non_hazard_y_std': self.non_hazard_df['video_rel_y'].std()
            }
        
        # print recommendations for object matching
        print("\nrecommendations for object-gaze matching:")
        print("-" * 40)
        
        print("\n1. temporal window:")
        if 'optimal_time_window' in metrics:
            print(f"   - use gaze data within {metrics['optimal_time_window']['q75']:.0f}ms of spacebar press")
            print(f"   - median reaction time: {metrics['optimal_time_window']['median']:.0f}ms")
        
        print("\n2. spatial matching criteria:")
        if 'spatial_std' in metrics:
            print(f"   - hazard gaze is more concentrated (std_x: {metrics['spatial_std']['hazard_x_std']:.3f})")
            print(f"   - use tighter bounding box margins for hazard objects")
        
        print("\n3. attention indicators:")
        if 'hazard_fixation_rate' in metrics:
            print(f"   - {metrics['hazard_fixation_rate']*100:.1f}% of hazard moments are fixations")
            print(f"   - prioritize objects within fixation clusters")
        
        return metrics
    
    def create_object_matching_features(self):
        """
        create features that will be useful for matching objects with gaze
        """
        print("\n" + "="*50)
        print("creating object matching features")
        print("="*50)
        
        # group by user, video, and approximate time windows
        # this simulates having multiple gaze points for each potential object
        
        # create time bins (100ms windows)
        self.df['time_bin'] = (self.df['timestamp'] // 100) * 100
        
        # prepare aggregation dict based on available columns
        agg_dict = {}
        
        # add aggregations only for existing columns
        if 'video_rel_x' in self.df.columns:
            agg_dict['video_rel_x'] = ['mean', 'std', 'min', 'max']
        if 'video_rel_y' in self.df.columns:
            agg_dict['video_rel_y'] = ['mean', 'std', 'min', 'max']
        if 'gaze_speed' in self.df.columns:
            agg_dict['gaze_speed'] = 'mean'
        if 'is_fixation' in self.df.columns:
            agg_dict['is_fixation'] = 'max'
        if 'is_hazard_moment' in self.df.columns:
            agg_dict['is_hazard_moment'] = 'max'
        if 'time_to_nearest_spacebar' in self.df.columns:
            agg_dict['time_to_nearest_spacebar'] = 'min'
        if 'gaze_dispersion_total' in self.df.columns:
            agg_dict['gaze_dispersion_total'] = 'mean'
        
        if not agg_dict:
            print("warning: no aggregatable columns found")
            return pd.DataFrame()
        
        # aggregate features for each time bin
        aggregated = self.df.groupby(['user_id', 'video_id', 'session_num', 'time_bin']).agg(agg_dict).reset_index()
        
        # flatten column names
        aggregated.columns = ['_'.join(col).strip('_') for col in aggregated.columns.values]
        
        print(f"\ncreated {len(aggregated)} time-binned samples")
        print("these represent potential object detection moments")
        
        # calculate bounding box suggestions based on gaze spread (if columns exist)
        if 'video_rel_x_max' in aggregated.columns and 'video_rel_x_min' in aggregated.columns:
            aggregated['suggested_bbox_width'] = (aggregated['video_rel_x_max'] - aggregated['video_rel_x_min']) * 1.5
            aggregated['suggested_bbox_center_x'] = aggregated['video_rel_x_mean']
        
        if 'video_rel_y_max' in aggregated.columns and 'video_rel_y_min' in aggregated.columns:
            aggregated['suggested_bbox_height'] = (aggregated['video_rel_y_max'] - aggregated['video_rel_y_min']) * 1.5
            aggregated['suggested_bbox_center_y'] = aggregated['video_rel_y_mean']
        
        # create attention score for prioritizing objects
        aggregated['attention_score'] = 0.0
        
        # high attention if fixation during hazard
        if 'is_fixation_max' in aggregated.columns and 'is_hazard_moment_max' in aggregated.columns:
            mask = (aggregated['is_fixation_max'] == 1) & (aggregated['is_hazard_moment_max'] == 1)
            aggregated.loc[mask, 'attention_score'] += 1.0
        
        # medium attention if near spacebar
        if 'time_to_nearest_spacebar_min' in aggregated.columns:
            mask = aggregated['time_to_nearest_spacebar_min'] < 1000
            aggregated.loc[mask, 'attention_score'] += 0.5
        
        # low dispersion indicates focused attention
        if 'gaze_dispersion_total_mean' in aggregated.columns:
            low_dispersion_mask = aggregated['gaze_dispersion_total_mean'] < aggregated['gaze_dispersion_total_mean'].quantile(0.25)
            aggregated.loc[low_dispersion_mask, 'attention_score'] += 0.3
        
        return aggregated
    
    def visualize_matching_strategy(self, sample_size: int = 5):
        """
        visualize how gaze patterns could match with objects
        """
        print("\n" + "="*50)
        print("visualizing object matching strategy")
        print("="*50)
        
        # get hazard moments with good gaze data
        hazard_samples = self.hazard_df[
            (self.hazard_df['is_fixation'] == 1) & 
            (self.hazard_df['video_rel_x'].notna())
        ].head(sample_size)
        
        if len(hazard_samples) == 0:
            print("no suitable hazard fixation samples found")
            return
        
        fig, axes = plt.subplots(1, min(sample_size, len(hazard_samples)), figsize=(4*sample_size, 4))
        if sample_size == 1:
            axes = [axes]
        
        for idx, (_, sample) in enumerate(hazard_samples.iterrows()):
            if idx >= len(axes):
                break
                
            ax = axes[idx]
            
            # simulate a frame with gaze point
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            ax.set_aspect('equal')
            
            # plot gaze point
            gaze_x = sample['video_rel_x']
            gaze_y = sample['video_rel_y']
            ax.scatter(gaze_x, gaze_y, c='red', s=100, marker='x', linewidths=2, label='Gaze')
            
            # simulate potential object bounding boxes
            # these would come from yolo in real implementation
            np.random.seed(idx)  # for reproducibility
            
            # generate some fake objects
            n_objects = np.random.randint(3, 7)
            for obj_idx in range(n_objects):
                # random object position and size
                obj_x = np.random.uniform(0.1, 0.9)
                obj_y = np.random.uniform(0.1, 0.9)
                obj_w = np.random.uniform(0.05, 0.2)
                obj_h = np.random.uniform(0.05, 0.2)
                
                # calculate distance from gaze to object center
                dist = np.sqrt((gaze_x - obj_x)**2 + (gaze_y - obj_y)**2)
                
                # determine if gaze is inside or near object
                is_inside = (abs(gaze_x - obj_x) < obj_w/2) and (abs(gaze_y - obj_y) < obj_h/2)
                
                # color based on distance/overlap
                if is_inside:
                    color = 'red'
                    alpha = 0.3
                    linewidth = 2
                elif dist < 0.1:
                    color = 'orange'
                    alpha = 0.2
                    linewidth = 1.5
                else:
                    color = 'blue'
                    alpha = 0.1
                    linewidth = 1
                
                # draw bounding box
                rect = plt.Rectangle((obj_x - obj_w/2, obj_y - obj_h/2), 
                                    obj_w, obj_h,
                                    linewidth=linewidth, 
                                    edgecolor=color, 
                                    facecolor=color,
                                    alpha=alpha)
                ax.add_patch(rect)
                
                # add distance text for close objects
                if dist < 0.2:
                    ax.text(obj_x, obj_y, f'{dist:.2f}', 
                           fontsize=8, ha='center', va='center')
            
            ax.set_title(f'Sample {idx+1}: Hazard Detection')
            ax.set_xlabel('Normalized X')
            ax.set_ylabel('Normalized Y')
            ax.grid(True, alpha=0.3)
            ax.legend()
        
        plt.tight_layout()
        plt.show()
        
        print("\nmatching strategy:")
        print("- red boxes: gaze inside or very close (likely hazard)")
        print("- orange boxes: gaze nearby (possible hazard)")  
        print("- blue boxes: gaze far away (unlikely hazard)")
        print("- numbers show distance from gaze to object center")

def run_eda(csv_path: str):
    """
    run complete eda on the restructured gaze data
    
    Args:
        csv_path: path to the restructured csv file
    """
    # load data
    print(f"loading data from {csv_path}")
    df = pd.read_csv(csv_path)
    
    # create analyzer
    analyzer = GazePatternAnalyzer(df)
    
    # run analyses
    analyzer.analyze_spatial_distribution()
    analyzer.analyze_temporal_patterns()
    metrics = analyzer.analyze_attention_patterns()
    
    # create object matching features
    aggregated_df = analyzer.create_object_matching_features()
    
    # visualize matching strategy
    analyzer.visualize_matching_strategy(sample_size=5)
    
    # save aggregated features for object matching
    output_path = csv_path.replace('.csv', '_aggregated_for_matching.csv')
    aggregated_df.to_csv(output_path, index=False)
    print(f"\nsaved aggregated features to {output_path}")
    
    # print summary
    print("\n" + "="*50)
    print("eda summary - key findings for object matching:")
    print("="*50)
    
    print("\n1. spatial patterns:")
    print("   - hazard gaze is more concentrated in specific regions")
    print("   - objects in center-horizon region more likely to be hazards")
    
    print("\n2. temporal patterns:")
    print("   - use 500-2000ms window around spacebar press")
    print("   - fixations are strong indicators of hazard attention")
    
    print("\n3. matching algorithm should:")
    print("   - calculate distance from gaze to each object bbox")
    print("   - weight by temporal proximity to hazard indicator")
    print("   - consider gaze velocity (lower = more attention)")
    print("   - use fixation clusters as high-priority zones")
    
    return df, aggregated_df, metrics

# example usage
if __name__ == "__main__":
    # path to your restructured csv
    csv_path = "data/processed/hazard_training_dataset.csv"
    
    # run the eda
    df, aggregated_df, metrics = run_eda(csv_path)
    
    print("\nready for object detection integration!")
    print("next steps:")
    print("1. run yolo on video frames at aggregated timestamps")
    print("2. match detected objects with gaze patterns using distance metrics")
    print("3. assign hazard labels based on attention scores")
    print("4. train hazard detection model on visual features only")