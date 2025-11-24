'''
    This script logs basic file information for specified file types in a given parent directory.
    The script walks recursively through the directory to find files with the designated extensions
    and logs the lines of content within each file. Files are temporarily copied to a new file with a 
    "_temp.txt" suffix for line counting, and then deleted after processing.
'''

import os
import shutil
import json
from datetime import datetime

# Define extensions to look for
TARGET_EXTENSIONS = {'.tsx', '.ts', '.jsx', '.js', '.html', '.py', '.css', '.md', '.xml'}

# Get directory of this script (i.e., /src)
script_dir = os.path.dirname(os.path.abspath(__file__))

# Path to log file
log_file_path = os.path.join(script_dir, 'scriptLog.json')

# Load or create scriptLog.json
def initialize_log_file(path):
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"Loaded existing log file: {path}")
        except Exception as e:
            print(f"Error reading existing log file: {e}")
            data = {}
    else:
        data = {}
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        print(f"Created new log file: {path}")
    return data

def list_and_process_files(directory, log_data):
    lines = 0
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
    log_data[timestamp] = {}
    for root, _, files in os.walk(directory):
        for file in files:
            file_name, ext = os.path.splitext(file)
            if ext in TARGET_EXTENSIONS:
                full_path = os.path.join(root, file)
                txt_copy_path = os.path.join(root, f"{file_name}_temp.txt")

                try:
                    shutil.copyfile(full_path, txt_copy_path)

                    with open(txt_copy_path, 'r', encoding='utf-8', errors='ignore') as f:
                        line_count = sum(1 for line in f if line.strip())  # Count non-blank lines

                    # Print summary
                    print(f"[{timestamp}] File: {file}")
                    print(f"Path: {full_path}")
                    print(f"Line Count: {line_count}\n")

                    # Log data to the json log
                    log_data[timestamp][file] = {
                        "file_name": file_name,
                        "ext": ext,
                        "file_path": full_path,
                        "lines": line_count,
                    }
                    lines+=line_count

                except Exception as e:
                    print(f"Error processing {file}: {e}")

                finally:
                    if os.path.exists(txt_copy_path):
                        os.remove(txt_copy_path)
    print("TOTAL LINES: ", lines)
    return log_data

if __name__ == "__main__":
    print(f"Scanning directory: {script_dir}\n")
    log_data = initialize_log_file(log_file_path)
    updated_log = list_and_process_files(script_dir, log_data)

    # Save log to scriptLog.json
    with open(log_file_path, 'w', encoding='utf-8') as f:
        json.dump(updated_log, f, indent=2)
    print(f"\nUpdated log saved to: {log_file_path}")
