import os
import json
import zipfile
import shutil
import geopandas as gpd
import pyproj as proj
from pyproj import Transformer
import fiona
######################################################################################
######################################################################################
# provide the folder name and the shapefile zip names to process
queryFolderName = "tiger"
#shapefileZipNames = ["ASC_Art_Installations", "Churches", "Colleges", "Common_Area_Parcels", "Discgolf_Fairway", "Discgolf_Tees", "Golf_Courses", "Greenway_Entrances", "GreenwayMasterPlan", "Greenways"]
shapefileZipNames = ['tl_2024_us_county']
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
idx_src = split_dir.index("src")
parent_dir = split_dir[idx_src-1]
######################################################################################
def list_folders(directory):
    try:
        # List only directories
        folders = [f for f in os.listdir(directory) if os.path.isdir(os.path.join(directory, f))]
        return folders
    except FileNotFoundError:
        return f"Error: The directory '{directory}' does not exist."
    except PermissionError:
        return f"Error: Permission denied for directory '{directory}'."
######################################################################################
def create_folder(path):
    try:
        os.makedirs(path, exist_ok=True)  # Creates the folder and any intermediate folders if needed
        print(f"Folder '{path}' created successfully!")
    except PermissionError:
        print(f"Error: Permission denied for creating folder '{path}'.")
    except FileExistsError:
        print(f"Error: Folder '{path}' already exists.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
######################################################################################
######################################################################################

def list_files_in_directory(directory_path: str):
    try:
        # Get a list of all files in the given directory
        files = [file for file in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, file))]
        return files
    except FileNotFoundError:
        print(f"The directory {directory_path} does not exist.")
        return []
    except PermissionError:
        print(f"Permission denied for accessing {directory_path}.")
        return []

if len(shapefileZipNames) == 0:
    files = list_files_in_directory(os.path.join(r"data", queryFolderName))
    for f in files:
        if f.endswith('.zip'):
           shapefileZipNames.append(f.split(".")[0])
######################################################################################
folders = list_folders(os.path.join(parent_dir, "output"))
if queryFolderName not in folders:
    folder_path = os.path.join(parent_dir, "output", queryFolderName)
    create_folder(folder_path)


for shapefileZipName in shapefileZipNames:

    zip_file_path =  os.path.join(parent_dir, "data", queryFolderName, shapefileZipName+".zip")
    geojson_output_path = os.path.join(parent_dir, "output", queryFolderName, shapefileZipName+".geojson")
    #print(zip_file_path, geojson_output_path)

    def shapefile_to_geojson(zip_path, output_geojson_path):
        """
        Convert a shapefile (in .zip format) to GeoJSON.
        
        Parameters:
        - zip_path: Path to the .zip file containing shapefile components.
        - output_geojson_path: Path to the output GeoJSON file.
        """
        try:
            print(f"{shapefileZipName} is being processed...")
            # Open the ZIP file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # List files in the root of the ZIP file (not in subfolders)
                root_files = [f for f in zip_ref.namelist() if "/" not in f]
                
                # Find the .shp file in the root
                shp_file_name = next((f for f in root_files if f.endswith('.shp')), None)
                
                if not shp_file_name:
                    raise FileNotFoundError(
                        "No .shp file found in the main directory of the provided .zip archive.")
                
                # Extract the .shp file and its associated files to memory
                shp_related_files = [
                    f for f in root_files if 
                    f.startswith(os.path.splitext(shp_file_name)[0])
                    ]
                
                temp_extract_dir = os.path.join(parent_dir, "data", queryFolderName, "temp_extract")
                os.makedirs(temp_extract_dir, exist_ok=True)

                for file in shp_related_files:
                    zip_ref.extract(file, temp_extract_dir)

                # Build the full path to the extracted .shp file
                shp_file_path = os.path.join(temp_extract_dir, shp_file_name)
                
                # Read the shapefile using GeoPandas
                gdf = gpd.read_file(shp_file_path)
                
                # Convert to GeoJSON and save
                gdf.to_file(output_geojson_path, driver='GeoJSON')
        
        finally:
            # Ensure the extracted temporary directory is removed
            if os.path.exists("temp_extract"):
                shutil.rmtree("temp_extract")

    # Example usage
    shapefile_to_geojson(zip_file_path, geojson_output_path)
    print(f"{shapefileZipName} has been converted to GeoJSON")
    ##################################################################################

    transformer = Transformer.from_crs(
        proj.CRS('epsg:2264'), proj.CRS('epsg:4326'), always_xy=True)

    output = {
        "type": "FeatureCollection",
        "features": []
    }

    with fiona.open(geojson_output_path, mode="r") as src:
        print("Total Features:", len(src))
        print("Schema:", src.schema)

        # Iterate through features
        for feature in src:
            try:
                properties = {}
                for key, value in feature["properties"].items():
                    properties[key] = value

                geometry = feature["geometry"]
                newFeature = {
                    "type": "Feature",
                    "properties": properties,
                    "geometry": {
                        "type": geometry["type"],
                        "coordinates": []
                    }
                }
                ##########################################################################
                #print(geometry["type"], geometry["coordinates"])
                if geometry["type"] in ["Polygon", "MultiPolygon"]:
                    # Transform all coordinate rings (exterior and any holes)
                    coordinates = geometry["coordinates"]
                    poolCoords = [[],[]]
                    for ring in coordinates:
                        transformed_ring = [
                            transformer.transform(coord[0], coord[1]) for coord in ring
                        ]
                        #print("Transformed Ring (EPSG:4326):", transformed_ring)
                        for coord in transformed_ring:
                            poolCoords[0].append(coord[0])
                            poolCoords[1].append(coord[1])
                        newFeature["geometry"]["coordinates"].append(transformed_ring)
                    
                    centroid = [
                        sum(poolCoords[1])/len(poolCoords[1]), 
                        sum(poolCoords[0])/len(poolCoords[0])]
                    
                    newFeature["properties"]["centroid"] = centroid
                    output["features"].append(newFeature)

                elif geometry["type"] == "LineString":
                    # Transform all coordinate rings (exterior and any holes)
                    coordinates = geometry["coordinates"]
                    poolCoords = [[],[]]
                    transformed_linestring = []
                    for coord in coordinates:
                        transformed_coordinates = transformer.transform(coord[0], coord[1])
                        poolCoords[0].append(transformed_coordinates[0])
                        poolCoords[1].append(transformed_coordinates[1])
                        transformed_linestring.append(transformed_coordinates)
                    newFeature["geometry"]["coordinates"] = transformed_linestring
                    
                    centroid = [
                        sum(poolCoords[1])/len(poolCoords[1]), 
                        sum(poolCoords[0])/len(poolCoords[0])]
                    
                    newFeature["properties"]["centroid"] = centroid
                    output["features"].append(newFeature)

                elif geometry["type"] == "Point":
                    coordinates = geometry["coordinates"]
                    transformed_coordinates = transformer.transform(coordinates[0], coordinates[1])
                    newFeature["properties"]["centroid"] = [transformed_coordinates[0], transformed_coordinates[1]]
                    newFeature["geometry"]["coordinates"] = [transformed_coordinates[0], transformed_coordinates[1]]
                    output["features"].append(newFeature)
            except Exception as e:
                print(e)
                pass

    try:
        with open(os.path.join(parent_dir, "output", shapefileZipName+".geojson"), "w", encoding='utf-8') as output_json:
            output_json.write(json.dumps(output, indent=1, ensure_ascii=False))
        print(f"{shapefileZipName} : transformed to EPSG:4326")
    except Exception as e:
        print(e)
        pass

print('DONE')
