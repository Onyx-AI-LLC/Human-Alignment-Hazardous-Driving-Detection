import os
import shutil
import re

def get_highest_video_number(output_folder):
    """
    Find the highest video number in the output folder to continue numbering from there.
    
    :param output_folder: Path to check for existing video files
    :return: Highest video number found, or 0 if none exist
    """
    if not os.path.exists(output_folder):
        return 0
    
    highest = 0
    for filename in os.listdir(output_folder):
        # Match pattern like "video123.mp4"
        match = re.match(r'video(\d+)\.mp4', filename)
        if match:
            number = int(match.group(1))
            highest = max(highest, number)
    
    return highest

def process_videos(input_folders, output_folder):
    """
    Processes video files from multiple folders, renames duplicates uniquely, 
    and saves them sequentially in an output folder, continuing from the highest existing number.
    
    :param input_folders: List of folder paths containing videos.
    :param output_folder: Path to the output folder where renamed videos will be saved.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Find the highest existing video number and start from the next one
    highest_existing = get_highest_video_number(output_folder)
    counter = highest_existing + 1
    
    print(f"Found highest existing video number: {highest_existing}")
    print(f"Starting new videos from: video{counter}.mp4")

    seen_files = {}
    video_list = []

    # Iterate over each folder and file
    for folder in input_folders:
        for file in os.listdir(folder):
            if file.endswith(('.mp4', '.avi', '.mov', '.mkv')):  # Add more formats if needed
                file_path = os.path.join(folder, file)

                # Check for duplicate filenames
                if file in seen_files:
                    base, ext = os.path.splitext(file)
                    new_name = f"{base}_{len(seen_files[file]) + 1}{ext}"
                    seen_files[file].append(new_name)
                else:
                    new_name = file
                    seen_files[file] = [new_name]

                # Store file path and new name for sequential renaming
                video_list.append((file_path, new_name))

    # Rename sequentially
    for file_path, _ in video_list:
        new_filename = f"video{counter}.mp4"
        new_filepath = os.path.join(output_folder, new_filename)
        shutil.copy(file_path, new_filepath)
        print(f"Renamed {file_path} -> {new_filename}")
        counter += 1

# Example usage - Updated for your current setup
input_folders = ["/Users/lennoxanderson/Documents/Output2/"]  # Your subfolder with new videos
output_folder = "/Users/lennoxanderson/Documents/Research/Human-Alignment-Hazardous-Driving-Detection/data/raw/videos/"  # Your main data folder

if __name__ == "__main__":
    print(f"Combining videos from subfolder: {input_folders[0]}")
    print(f"Adding to main folder: {output_folder}")
    process_videos(input_folders, output_folder)
