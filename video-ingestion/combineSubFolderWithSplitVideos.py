import os
import shutil

def process_videos(input_folders, output_folder):
    """
    Processes video files from multiple folders, renames duplicates uniquely, 
    and saves them sequentially in an output folder.
    
    :param input_folders: List of folder paths containing videos.
    :param output_folder: Path to the output folder where renamed videos will be saved.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    seen_files = {}
    video_list = []
    counter = 1

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

# Example usage
input_folders = ["/Users/lennoxanderson/Documents/machineLearning/data/TeslaRawDrivingFootage/SplitData/FirstBatchSplit", "/Users/lennoxanderson/Documents/machineLearning/data/TeslaRawDrivingFootage/SplitData/SecondBatchSplit", "/Users/lennoxanderson/Documents/machineLearning/data/TeslaRawDrivingFootage/SplitData/ThirdBatchSplit", "/Users/lennoxanderson/Documents/machineLearning/data/TeslaRawDrivingFootage/SplitData/FourthBatchSplit"]  # Replace with actual folder paths
output_folder = "/Users/lennoxanderson/Documents/machineLearning/data/TeslaRawDrivingFootage/SplitData/1-4BatchSplits"
process_videos(input_folders, output_folder)
