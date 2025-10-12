#!/usr/bin/env python3
"""
Sync local videos to S3 by comparing what exists locally vs in S3
Only uploads videos that are missing from S3 to avoid duplicates
"""

import boto3
import os
from pathlib import Path
from datetime import datetime
import re
from dotenv import load_dotenv

load_dotenv()

class VideoS3Sync:
    def __init__(self, bucket_name='hahd-primary-data-storage'):
        """
        Initialize S3 client and set paths
        """
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        self.bucket_name = bucket_name
        self.s3_prefix = 'raw/videos/'
        
        # Get project root and local video directory
        project_root = Path(__file__).parent.parent
        self.local_video_dir = project_root / 'data' / 'raw' / 'videos'
        
        print(f"Local video directory: {self.local_video_dir}")
        print(f"S3 bucket: {self.bucket_name}")
        print(f"S3 prefix: {self.s3_prefix}")

    def get_local_videos(self):
        """
        Get list of video files in local directory
        Returns: set of video filenames
        """
        if not self.local_video_dir.exists():
            print(f"Error: Local video directory does not exist: {self.local_video_dir}")
            return set()
        
        local_videos = set()
        video_extensions = {'.mp4', '.avi', '.mov', '.mkv'}
        
        for file_path in self.local_video_dir.iterdir():
            if file_path.is_file() and file_path.suffix.lower() in video_extensions:
                local_videos.add(file_path.name)
        
        print(f"Found {len(local_videos)} videos locally")
        return local_videos

    def get_s3_videos(self):
        """
        Get list of video files in S3 bucket
        Returns: set of video filenames (without prefix)
        """
        s3_videos = set()
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=self.s3_prefix
            )
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        # Extract filename from key (remove prefix)
                        if key.startswith(self.s3_prefix):
                            filename = key[len(self.s3_prefix):]
                            if filename:  # Skip empty names (like directory entries)
                                s3_videos.add(filename)
            
            print(f"Found {len(s3_videos)} videos in S3")
            
        except Exception as e:
            print(f"Error listing S3 videos: {e}")
            return set()
        
        return s3_videos

    def find_videos_to_upload(self):
        """
        Compare local and S3 videos to find what needs to be uploaded
        Returns: set of filenames that need to be uploaded
        """
        print("\nComparing local videos with S3...")
        
        local_videos = self.get_local_videos()
        s3_videos = self.get_s3_videos()
        
        # Find videos that exist locally but not in S3
        videos_to_upload = local_videos - s3_videos
        
        print(f"\nComparison Results:")
        print(f"  Local videos: {len(local_videos)}")
        print(f"  S3 videos: {len(s3_videos)}")
        print(f"  Videos to upload: {len(videos_to_upload)}")
        
        if videos_to_upload:
            print(f"\nVideos that need to be uploaded:")
            
            # Sort the videos by number for better readability
            def extract_number(filename):
                match = re.search(r'video(\d+)', filename)
                return int(match.group(1)) if match else float('inf')
            
            sorted_videos = sorted(videos_to_upload, key=extract_number)
            
            # Show first 10 and last 10 if there are many
            if len(sorted_videos) <= 20:
                for video in sorted_videos:
                    print(f"    {video}")
            else:
                print("    First 10:")
                for video in sorted_videos[:10]:
                    print(f"      {video}")
                print(f"    ... ({len(sorted_videos) - 20} more) ...")
                print("    Last 10:")
                for video in sorted_videos[-10:]:
                    print(f"      {video}")
        
        return videos_to_upload

    def upload_missing_videos(self, videos_to_upload, dry_run=True):
        """
        Upload videos that are missing from S3
        
        Args:
            videos_to_upload: set of video filenames to upload
            dry_run: if True, only show what would be uploaded without actually uploading
        """
        if not videos_to_upload:
            print("\nNo videos need to be uploaded - S3 is already in sync!")
            return
        
        if dry_run:
            print(f"\nDRY RUN MODE - Would upload {len(videos_to_upload)} videos:")
            for video in sorted(videos_to_upload):
                local_path = self.local_video_dir / video
                s3_key = self.s3_prefix + video
                file_size_mb = local_path.stat().st_size / (1024 * 1024)
                print(f"  Would upload: {video} ({file_size_mb:.1f} MB) -> s3://{self.bucket_name}/{s3_key}")
            
            print(f"\nTo actually upload these videos, run with dry_run=False")
            return
        
        # Actually upload the videos
        print(f"\nStarting upload of {len(videos_to_upload)} videos...")
        
        uploaded_count = 0
        failed_uploads = []
        total_size_mb = 0
        
        for video in sorted(videos_to_upload):
            local_path = self.local_video_dir / video
            s3_key = self.s3_prefix + video
            
            try:
                file_size_mb = local_path.stat().st_size / (1024 * 1024)
                print(f"Uploading: {video} ({file_size_mb:.1f} MB)...")
                
                self.s3_client.upload_file(
                    str(local_path),
                    self.bucket_name,
                    s3_key
                )
                
                uploaded_count += 1
                total_size_mb += file_size_mb
                print(f"  Success: {video}")
                
            except Exception as e:
                print(f"  Failed: {video} - {e}")
                failed_uploads.append(video)
        
        # Summary
        print(f"\nUpload Summary:")
        print(f"  Successfully uploaded: {uploaded_count}/{len(videos_to_upload)} videos")
        print(f"  Total size uploaded: {total_size_mb:.2f} MB ({total_size_mb/1024:.2f} GB)")
        print(f"  Failed uploads: {len(failed_uploads)}")
        
        if failed_uploads:
            print(f"  Failed videos:")
            for video in failed_uploads:
                print(f"    {video}")

    def sync_videos(self, dry_run=True):
        """
        Main function to sync local videos to S3
        
        Args:
            dry_run: if True, only show what would be uploaded without actually uploading
        """
        print("Starting video sync to S3...")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Find videos that need to be uploaded
        videos_to_upload = self.find_videos_to_upload()
        
        # Upload missing videos
        self.upload_missing_videos(videos_to_upload, dry_run=dry_run)
        
        print(f"\nVideo sync completed!")


def main():
    """
    Main function with user interaction
    """
    sync = VideoS3Sync()
    
    # First, do a dry run to show what would be uploaded
    print("=" * 60)
    print("STEP 1: DRY RUN - Checking what needs to be uploaded")
    print("=" * 60)
    
    videos_to_upload = sync.find_videos_to_upload()
    
    if not videos_to_upload:
        print("\nAll videos are already synced to S3!")
        return
    
    # Show what would be uploaded
    sync.upload_missing_videos(videos_to_upload, dry_run=True)
    
    # Ask user if they want to proceed
    print("\n" + "=" * 60)
    print("STEP 2: CONFIRM UPLOAD")
    print("=" * 60)
    
    response = input("\nDo you want to proceed with uploading these videos to S3? (y/N): ").strip().lower()
    
    if response in ['y', 'yes']:
        print("\n" + "=" * 60)
        print("STEP 3: UPLOADING VIDEOS")
        print("=" * 60)
        sync.upload_missing_videos(videos_to_upload, dry_run=False)
    else:
        print("\nUpload cancelled by user.")


if __name__ == "__main__":
    main()