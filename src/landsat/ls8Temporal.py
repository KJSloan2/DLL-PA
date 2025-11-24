import os
import numpy as np
import json
import csv

######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import get_files
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
split_dir = str(script_dir).split("/")
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]
######################################################################################
data_dir = os.path.join(parent_dir, "output", "landsat", locationKey, "arrays")
######################################################################################
metricKeys = ["lstf", "ndvi", "ndmi"]
analysisStack = {}

lstfRanges = []
lstfStep = 2
for i in range(40, 141, lstfStep):
    layer_range = [i - 1, i + lstfStep + 1]
    lstfRanges.append(layer_range)

lstfChangeRanges = []
lstfChangeStep = 5
for i in range(40, 141, lstfStep):
    layer_range = [i - 1, i + lstfStep + 1]
    lstfRanges.append(layer_range)

######################################################################################
ndviRanges = []
ndviStep = 0.1
for i in np.arange(-0.5, 1.2, ndviStep):
    i = float(i)
    layer_range = [i - 1, i + ndviStep + 1]
    ndviRanges.append(layer_range)
######################################################################################
ndmiRanges = []
ndmiStep = 0.1
for i in np.arange(-0.5, 1.2, ndmiStep):
    i = float(i)
    layer_range = [i - 1, i + ndmiStep + 1]
    ndmiRanges.append(layer_range)
######################################################################################
arrayFiles = get_files(data_dir)

for metricKey in metricKeys:
    prefix = f"{metricKey}_{locationKey}"
    suffix = ".npy"
    start_year = 2013
    end_year = 2024
    years = list(range(start_year, end_year + 1))

    # Load all yearly arrays into a 3D stack (time, rows, cols)
    data_stack = []
    for year in years:
        try:
            path = os.path.join(data_dir, f"{prefix}_{year}{suffix}")
            # Add allow_pickle=True parameter
            arr = np.load(path, allow_pickle=True)
            data_stack.append(arr)
        except Exception as e:
            print(f"Error loading file for year {year}: {path}")
            print(f"Error: {e}")
            continue
        
    data_stack = np.stack(data_stack, axis=0)
    diff_stack = np.diff(data_stack, axis=0)
    overall_change = data_stack[-1] - data_stack[0]
    meanYoyChange = overall_change / (end_year - start_year)
    
    analysisStack[f"{metricKey}_data"] = data_stack
    analysisStack[f"{metricKey}_diff"] = diff_stack
    analysisStack[f"{metricKey}_overall_change"] = overall_change
    analysisStack[f"{metricKey}_mean_yoy_change"] = meanYoyChange

    print("data_stack shape: ", data_stack.shape)
    print("diff_stack shape: ", diff_stack.shape)
    print("overall_change shape: ", overall_change.shape)
    print("meanYoyChange shape: ", meanYoyChange.shape)

    rows, cols = data_stack.shape[1], data_stack.shape[2]
    pixel_time_series = [[data_stack[:, i, j] for j in range(cols)] for i in range(rows)]

coords_path = os.path.join(data_dir, f"geo_{locationKey}{suffix}")
coords_arr = np.load(coords_path, allow_pickle=True)
print("coords_arr shape: ", coords_arr.shape)

'''temporalComposite = {
    "lat":[], "lon":[], 
    "lstf":[], "lstf_diff":[], "lstf_total_change":[], "lstf_mean_yoy_change":[], 
    "ndvi":[], "ndvi_diff":[], "ndvi_total_change":[], "ndvi_mean_yoy_change":[],
    }'''

deciRound = 1
with open(os.path.join(data_dir, f"ls8_{locationKey}_temporal.csv"), mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    #writer.writerow(list(temporalComposite.keys()))
    writer.writerow(["lon", "lat",
                    "lstf", "lstf_total_change", "lstf_mean_yoy_change", 
                    "ndvi", "ndvi_total_change", "ndvi_mean_yoy_change",
                    "ndmi", "ndmi_total_change", "ndmi_mean_yoy_change"
                    ])

    for i in range(coords_arr.shape[0]):
        for j in range(coords_arr.shape[1]):
            coords = coords_arr[i][j]
            if len(coords) != 0:
                lat = coords_arr[i][j][0]
                lon = coords_arr[i][j][1]
                lstf = analysisStack["lstf_data"][:, i, j].tolist()
                lstf = np.mean(list(map(lambda x: round((x), deciRound), lstf)))

                ndvi = analysisStack["ndvi_data"][:, i, j].tolist()
                ndvi = np.mean(list(map(lambda x: round((x), deciRound), ndvi)))

                # lstf difference
                lstf_diff = analysisStack["lstf_diff"][:, i, j].tolist()
                lstf_diff = list(map(lambda x: round((x), deciRound), lstf_diff))
                # lstf overall change
                lstf_total_change = analysisStack["lstf_overall_change"][i, j]
                #lstf_total_change = list(map(lambda x: round((x), deciRound), lstf_total_change))
                # lstf mean year over year change
                lstf_mean_yoy_change = analysisStack["lstf_mean_yoy_change"][i, j]
                #lstf_mean_yoy_change = list(map(lambda x: round((x), deciRound), lstf_mean_yoy_change))
                # ndvi difference
                ndvi_diff = analysisStack["ndvi_diff"][:, i, j].tolist()
                ndvi_diff = list(map(lambda x: round((x), deciRound), ndvi_diff))
                # ndvi overall change
                ndvi_total_change = analysisStack["ndvi_overall_change"][i, j].tolist()
                #ndvi_total_change = list(map(lambda x: round((x), deciRound), ndvi_total_change))
                # ndvi mean year over year change
                ndvi_mean_yoy_change = analysisStack["ndvi_mean_yoy_change"][i, j].tolist()
                #ndvi_mean_yoy_change = list(map(lambda x: round((x), deciRound), ndvi_mean_yoy_change))

                ndmi = analysisStack["ndmi_data"][:, i, j].tolist()
                ndmi = np.mean(list(map(lambda x: round((x), deciRound), ndmi)))
                ndmi_diff = analysisStack["ndmi_diff"][:, i, j].tolist()
                ndmi_total_change = analysisStack["ndmi_overall_change"][i, j].tolist()
                ndmi_mean_yoy_change = analysisStack["ndmi_mean_yoy_change"][i, j].tolist()

                # Append to temporalComposite
                '''temporalComposite["lat"].append(lat)
                temporalComposite["lon"].append(lon)
                temporalComposite["lstf"].append(lstf)
                temporalComposite["ndvi"].append(ndvi)'''

                lstfRangeId = None
                for idx, lstfRange in enumerate(lstfRanges):  # Changed i to idx
                    if lstf >= lstfRange[0] and lstf < lstfRange[1]:
                        lstfRangeId = idx
                        break

                ndviRangeId = None
                for idx, ndviRange in enumerate(ndviRanges):  # Changed i to idx
                    if ndvi >= ndviRange[0] and ndvi < ndviRange[1]:
                        ndviRangeId = idx
                        break

                ndmiRangeId = None
                for idx, ndmiRange in enumerate(ndmiRanges):  # Changed i to idx
                    if ndmi >= ndmiRange[0] and ndmi < ndmiRange[1]:
                        ndmiRangeId = idx
                        break

                writer.writerow([
                    lat, lon, 
                    round((lstf),2), round((lstf_total_change),2), round((lstf_mean_yoy_change),2), 
                    round((ndvi),2), round((ndvi_total_change),2), round((ndvi_mean_yoy_change),2),
                    round((ndmi),2), round((ndmi_total_change),2), round((ndmi_mean_yoy_change),2)
                ])
                
                '''writer.writerow([lat, lon, lstf, lstf_diff, lstf_total_change, lstf_mean_yoy_change,
                                 ndvi, ndvi_diff, ndvi_total_change, ndvi_mean_yoy_change])'''
print("ls8Temporal.py DONE")