"""
Analyzes screen dimensions and viewport sizes from survey data to understand display characteristics.
Extracts and processes window dimensions from raw survey results and creates comprehensive visualizations.
Provides recommendations for standardized viewport dimensions based on usage patterns.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
import ast
import re
import warnings
from collections import Counter
import os

warnings.filterwarnings('ignore')

def load_and_parse_survey_data(filepath):
    """
    load survey results csv and parse the window dimensions
    """
    print("loading survey data...")
    df = pd.read_csv(filepath)
    print(f"loaded {len(df)} records")
    
    return df

def extract_window_dimensions_v2(df):
    """
    extract width and height from windowDimensions column - handles multiple formats
    """
    dimensions = []
    parsing_errors = []
    
    for idx, row in df.iterrows():
        try:
            window_dim_str = str(row['windowDimensions'])
            
            # method 1: try to extract using regex if it looks like a dict string
            width_match = re.search(r"'width':\s*(\d+)", window_dim_str)
            height_match = re.search(r"'height':\s*(\d+)", window_dim_str)
            
            if width_match and height_match:
                width = int(width_match.group(1))
                height = int(height_match.group(1))
                
                dimensions.append({
                    'record_id': row['_id'],
                    'user_id': row['userId'],
                    'video_id': row['videoId'],
                    'width': width,
                    'height': height,
                    'aspect_ratio': round(width/height, 3)
                })
                continue
            
            # method 2: try json parsing after cleaning
            try:
                # replace single quotes with double quotes and clean objectid references
                cleaned_str = window_dim_str.replace("'", '"')
                cleaned_str = re.sub(r'ObjectId\([^)]+\)', '""', cleaned_str)
                
                window_dict = json.loads(cleaned_str)
                width = int(window_dict['width'])
                height = int(window_dict['height'])
                
                dimensions.append({
                    'record_id': row['_id'],
                    'user_id': row['userId'],
                    'video_id': row['videoId'],
                    'width': width,
                    'height': height,
                    'aspect_ratio': round(width/height, 3)
                })
                continue
                
            except (json.JSONDecodeError, KeyError, ValueError):
                pass
            
            # method 3: try ast.literal_eval on cleaned string
            try:
                # remove objectid references
                cleaned_str = re.sub(r"ObjectId\([^)]+\)", "''", window_dim_str)
                window_dict = ast.literal_eval(cleaned_str)
                
                width = int(window_dict['width'])
                height = int(window_dict['height'])
                
                dimensions.append({
                    'record_id': row['_id'],
                    'user_id': row['userId'],
                    'video_id': row['videoId'],
                    'width': width,
                    'height': height,
                    'aspect_ratio': round(width/height, 3)
                })
                continue
                
            except (ValueError, SyntaxError, KeyError):
                pass
            
            # if none of the methods worked, log the error
            parsing_errors.append({
                'row': idx,
                'content': window_dim_str[:100],  # first 100 chars for debugging
                'error': 'could not parse with any method'
            })
                
        except Exception as e:
            parsing_errors.append({
                'row': idx,
                'content': str(row['windowDimensions'])[:100],
                'error': str(e)
            })
            continue
    
    dimensions_df = pd.DataFrame(dimensions)
    
    print(f"successfully parsed {len(dimensions_df)} records")
    if parsing_errors:
        print(f"failed to parse {len(parsing_errors)} records")
        # show first few parsing errors for debugging
        print("\nfirst few parsing errors:")
        for error in parsing_errors[:3]:
            print(f"  row {error['row']}: {error['error']}")
            print(f"    content: {error['content']}")
    
    return dimensions_df

def debug_data_format(df, num_samples=5):
    """
    debug function to understand the data format
    """
    print("\n" + "="*50)
    print("debugging data format")
    print("="*50)
    
    print(f"dataframe columns: {list(df.columns)}")
    print(f"dataframe shape: {df.shape}")
    
    print(f"\nfirst {num_samples} windowDimensions entries:")
    for i in range(min(num_samples, len(df))):
        content = df.iloc[i]['windowDimensions']
        print(f"row {i}:")
        print(f"  type: {type(content)}")
        print(f"  content: {repr(str(content)[:200])}")
        print()

def analyze_screen_dimensions(dimensions_df):
    """
    analyze and report on screen dimension patterns
    """
    if len(dimensions_df) == 0:
        print("no valid dimension data to analyze!")
        return None, None
        
    print("\n" + "="*50)
    print("screen dimensions analysis")
    print("="*50)
    
    # unique dimensions
    unique_dims = dimensions_df[['width', 'height']].drop_duplicates()
    print(f"\nnumber of unique screen dimensions: {len(unique_dims)}")
    
    print("\nunique screen dimensions found:")
    dim_summary = []
    for _, row in unique_dims.iterrows():
        count = len(dimensions_df[(dimensions_df['width'] == row['width']) & 
                                 (dimensions_df['height'] == row['height'])])
        aspect_ratio = round(row['width']/row['height'], 3)
        percentage = round(count/len(dimensions_df)*100, 1)
        
        dim_summary.append({
            'dimensions': f"{row['width']}x{row['height']}",
            'width': row['width'],
            'height': row['height'],
            'aspect_ratio': aspect_ratio,
            'count': count,
            'percentage': percentage
        })
        print(f"  {row['width']}x{row['height']} (aspect: {aspect_ratio}) - {count} records ({percentage}%)")
    
    # convert to dataframe for easy manipulation
    dim_summary_df = pd.DataFrame(dim_summary).sort_values('count', ascending=False)
    
    # aspect ratio analysis
    aspect_counts = Counter(dimensions_df['aspect_ratio'])
    print(f"\nunique aspect ratios: {len(aspect_counts)}")
    print("top aspect ratios:")
    for aspect, count in aspect_counts.most_common(5):
        percentage = count/len(dimensions_df)*100
        print(f"  {aspect}: {count} records ({percentage:.1f}%)")
    
    # most common dimensions
    most_common = dim_summary_df.iloc[0]
    print(f"\nmost common screen dimension: {most_common['dimensions']} ({most_common['count']} records, {most_common['percentage']}%)")
    
    # statistics
    print(f"\nwidth statistics:")
    print(f"  min: {dimensions_df['width'].min()}px")
    print(f"  max: {dimensions_df['width'].max()}px")
    print(f"  mean: {dimensions_df['width'].mean():.1f}px")
    print(f"  median: {dimensions_df['width'].median()}px")
    print(f"  std: {dimensions_df['width'].std():.1f}px")
    
    print(f"\nheight statistics:")
    print(f"  min: {dimensions_df['height'].min()}px")
    print(f"  max: {dimensions_df['height'].max()}px")
    print(f"  mean: {dimensions_df['height'].mean():.1f}px")
    print(f"  median: {dimensions_df['height'].median()}px")
    print(f"  std: {dimensions_df['height'].std():.1f}px")
    
    return dim_summary_df, aspect_counts

def create_visualizations(dimensions_df):
    """
    create visualizations for screen dimension analysis
    """
    if len(dimensions_df) == 0:
        print("no data to visualize!")
        return
        
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    
    # distribution of widths
    axes[0, 0].hist(dimensions_df['width'], bins=15, alpha=0.7, color='skyblue', edgecolor='black')
    axes[0, 0].set_title('distribution of screen widths', fontsize=12, fontweight='bold')
    axes[0, 0].set_xlabel('width (pixels)')
    axes[0, 0].set_ylabel('frequency')
    axes[0, 0].grid(True, alpha=0.3)
    
    # distribution of heights  
    axes[0, 1].hist(dimensions_df['height'], bins=15, alpha=0.7, color='lightcoral', edgecolor='black')
    axes[0, 1].set_title('distribution of screen heights', fontsize=12, fontweight='bold')
    axes[0, 1].set_xlabel('height (pixels)')
    axes[0, 1].set_ylabel('frequency')
    axes[0, 1].grid(True, alpha=0.3)
    
    # aspect ratio distribution
    axes[0, 2].hist(dimensions_df['aspect_ratio'], bins=15, alpha=0.7, color='orange', edgecolor='black')
    axes[0, 2].set_title('distribution of aspect ratios', fontsize=12, fontweight='bold')
    axes[0, 2].set_xlabel('aspect ratio (width/height)')
    axes[0, 2].set_ylabel('frequency')
    axes[0, 2].grid(True, alpha=0.3)
    
    # scatter plot of width vs height
    scatter = axes[1, 0].scatter(dimensions_df['width'], dimensions_df['height'], 
                                alpha=0.6, color='green', s=50)
    axes[1, 0].set_title('width vs height scatter plot', fontsize=12, fontweight='bold')
    axes[1, 0].set_xlabel('width (pixels)')
    axes[1, 0].set_ylabel('height (pixels)')
    axes[1, 0].grid(True, alpha=0.3)
    
    # box plot of dimensions
    box_data = [dimensions_df['width'], dimensions_df['height']]
    box_plot = axes[1, 1].boxplot(box_data, labels=['width', 'height'], patch_artist=True)
    box_plot['boxes'][0].set_facecolor('skyblue')
    box_plot['boxes'][1].set_facecolor('lightcoral')
    axes[1, 1].set_title('width and height distributions', fontsize=12, fontweight='bold')
    axes[1, 1].set_ylabel('pixels')
    axes[1, 1].grid(True, alpha=0.3)
    
    # dimension frequency bar chart
    dim_counts = dimensions_df.groupby(['width', 'height']).size().sort_values(ascending=False)
    top_dims = dim_counts.head(8)  # top 8 most common
    
    # create labels for the bar chart
    labels = [f"{w}x{h}" for (w, h) in top_dims.index]
    
    bars = axes[1, 2].bar(range(len(top_dims)), top_dims.values, 
                         color='purple', alpha=0.7)
    axes[1, 2].set_title('most common screen dimensions', fontsize=12, fontweight='bold')
    axes[1, 2].set_xlabel('dimension')
    axes[1, 2].set_ylabel('frequency')
    axes[1, 2].set_xticks(range(len(top_dims)))
    axes[1, 2].set_xticklabels(labels, rotation=45, ha='right')
    axes[1, 2].grid(True, alpha=0.3)
    
    # add count labels on bars
    for i, bar in enumerate(bars):
        height = bar.get_height()
        axes[1, 2].text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height)}', ha='center', va='bottom', fontsize=8)
    
    plt.tight_layout()
    
    # Save to eda_figures directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    figures_dir = os.path.join(script_dir, 'eda_figures')
    os.makedirs(figures_dir, exist_ok=True)
    plt.savefig(os.path.join(figures_dir, 'screen_dimensions_viewport_analysis.png'), dpi=300, bbox_inches='tight')
    plt.show()

def recommend_target_dimensions(dimensions_df, dim_summary_df):
    """
    recommend target dimensions for standardization
    """
    if len(dimensions_df) == 0 or dim_summary_df is None:
        print("no data available for recommendations!")
        return None
        
    print("\n" + "="*50)
    print("recommendations for standardization")
    print("="*50)
    
    # most common dimensions
    most_common = dim_summary_df.iloc[0]
    print(f"most common screen dimensions: {most_common['dimensions']}")
    print(f"  - used by {most_common['count']} records ({most_common['percentage']}%)")
    print(f"  - aspect ratio: {most_common['aspect_ratio']}")
    
    # calculate statistics for standardization decision
    records_needing_resize = len(dimensions_df) - most_common['count']
    print(f"\nif standardizing to most common dimensions ({most_common['dimensions']}):")
    print(f"  - records needing resize: {records_needing_resize} ({records_needing_resize/len(dimensions_df)*100:.1f}%)")
    print(f"  - records already correct: {most_common['count']} ({most_common['percentage']}%)")
    
    # alternative recommendations
    median_width = int(dimensions_df['width'].median())
    median_height = int(dimensions_df['height'].median())
    median_aspect = round(median_width/median_height, 3)
    
    print(f"\nalternative option (median-based dimensions):")
    print(f"  - dimensions: {median_width}x{median_height}")
    print(f"  - aspect ratio: {median_aspect}")
    
    # final recommendation
    print(f"\n{'='*20} final recommendation {'='*20}")
    if most_common['percentage'] >= 30:
        recommended_dims = (most_common['width'], most_common['height'])
        print(f"recommend standardizing to: {most_common['dimensions']}")
        print(f"reason: most common dimensions with {most_common['percentage']}% coverage")
    else:
        recommended_dims = (median_width, median_height)
        print(f"recommend standardizing to: {median_width}x{median_height}")
        print(f"reason: median-based dimensions provide good middle ground")
    
    return recommended_dims

def main():
    """
    main function to run the screen dimensions and viewport analysis
    """
    # Create output directories
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, 'eda_data')
    figures_dir = os.path.join(script_dir, 'eda_figures')
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(figures_dir, exist_ok=True)
    
    # file path - adjust as needed
    filepath = '../data/raw/survey_results_raw.csv'
    
    try:
        # load data
        df = load_and_parse_survey_data(filepath)
        
        # debug data format first
        debug_data_format(df)
        
        # extract dimensions using improved method
        dimensions_df = extract_window_dimensions_v2(df)
        
        if len(dimensions_df) == 0:
            print("no valid dimension data found! check the data format.")
            return None, None
            
        # analyze dimensions
        dim_summary_df, aspect_counts = analyze_screen_dimensions(dimensions_df)
        
        # create visualizations
        create_visualizations(dimensions_df)
        
        # get recommendations
        recommended_dims = recommend_target_dimensions(dimensions_df, dim_summary_df)
        
        # save processed dimensions data to eda_data directory
        dimensions_path = os.path.join(data_dir, 'screen_dimensions_analysis.csv')
        dimensions_df.to_csv(dimensions_path, index=False)
        
        if dim_summary_df is not None:
            summary_path = os.path.join(data_dir, 'screen_dimensions_summary.csv')
            dim_summary_df.to_csv(summary_path, index=False)
        
        print(f"\nanalysis complete!")
        print(f"saved processed data to:")
        print(f"  - {dimensions_path} ({len(dimensions_df)} records)")
        if dim_summary_df is not None:
            print(f"  - {summary_path} ({len(dim_summary_df)} unique dimensions)")
        print(f"  - {os.path.join(figures_dir, 'screen_dimensions_viewport_analysis.png')} (visualization)")
        
        return dimensions_df, recommended_dims
        
    except FileNotFoundError:
        print(f"file not found: {filepath}")
        print("please check the file path and try again.")
        return None, None
    except Exception as e:
        print(f"error occurred: {e}")
        import traceback
        traceback.print_exc()
        return None, None

if __name__ == "__main__":
    dimensions_df, recommended_dims = main()