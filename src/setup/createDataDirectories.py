import os

# Get project root based on script location
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
data_dir = os.path.join(project_root, 'backend', 'data')
output_dir = os.path.join(project_root, 'backend', 'output')
# List of subdirectories to create
subdirs = ['landsat', '3dep', 'uca']
for dir in [data_dir, output_dir]:
    for subdir in subdirs:
        dir_path = os.path.join(dir, subdir)
        os.makedirs(dir_path, exist_ok=True)
        print(f"Directory created or already exists: {dir_path}")