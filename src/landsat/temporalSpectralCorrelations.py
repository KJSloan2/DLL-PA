import os
import numpy as np
import json
import csv

from scipy.stats import pearsonr
import pandas as pd
import sqlite3
######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import get_files, to_py_type
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
conn = sqlite3.connect('tempGeo.db')
cursor = conn.cursor()
######################################################################################
metricKeys = ["lstf", "ndvi", "ndmi"]
analysisStack = {}

lstfRanges = []
lstfStep = 2
for i in range(50, 120, lstfStep):
    layer_range = [i - 1, i + lstfStep + 1]
    lstfRanges.append(layer_range)

lstfChangeRanges = []
lstfChangeStep = 5
for i in range(50, 120, lstfStep):
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

pixel_time_series = {}

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
            '''print(f"Error loading file for year {year}: {path}")
            print(f"Error: {e}")'''
            continue
        
    data_stack = np.stack(data_stack, axis=0)
    diff_stack = np.diff(data_stack, axis=0)
    overall_change = data_stack[-1] - data_stack[0]
    meanYoyChange = overall_change / (end_year - start_year)
    
    analysisStack[f"{metricKey}_data"] = data_stack
    analysisStack[f"{metricKey}_diff"] = diff_stack
    analysisStack[f"{metricKey}_serc"] = overall_change
    analysisStack[f"{metricKey}_arc"] = meanYoyChange

    rows, cols = data_stack.shape[1], data_stack.shape[2]
    #pixel_time_series = [[data_stack[:, i, j] for j in range(cols)] for i in range(rows)]

    pixel_time_series_np = {}
    for i in range(rows):
        for j in range(cols):
            pixel_time_series_np[(i, j)] = data_stack[:, i, j]

    analysisStack[f"{metricKey}_ts"] = pixel_time_series_np

stackArray_lstf = analysisStack["lstf_ts"]
stackArray_ndmi = analysisStack["ndmi_ts"]
stackArray_ndvi = analysisStack["ndvi_ts"]

correlation_results = {}

for (i, j) in stackArray_lstf.keys():
    val_lstf = stackArray_lstf[(i, j)]
    val_ndvi = stackArray_ndvi[(i, j)]
    val_ndmi = stackArray_ndmi[(i, j)]
    
    # Remove NaN values if present
    mask = ~(np.isnan(val_lstf) | np.isnan(val_ndvi) | np.isnan(val_ndmi))
    val_lstf_clean = val_lstf[mask]
    val_ndvi_clean = val_ndvi[mask]
    val_ndmi_clean = val_ndmi[mask]
    
    # Need at least 3 data points for correlation
    if len(val_lstf_clean) >= 3:
        try:
            # Calculate pairwise correlations
            corr_lstf_ndvi, p_lstf_ndvi = pearsonr(val_lstf_clean, val_ndvi_clean)
            corr_lstf_ndmi, p_lstf_ndmi = pearsonr(val_lstf_clean, val_ndmi_clean)
            corr_ndvi_ndmi, p_ndvi_ndmi = pearsonr(val_ndvi_clean, val_ndmi_clean)
            
            correlation_results[(i, j)] = {
                'lstf_ndvi_corr': corr_lstf_ndvi,
                'lstf_ndvi_pval': p_lstf_ndvi,
                'lstf_ndmi_corr': corr_lstf_ndmi,
                'lstf_ndmi_pval': p_lstf_ndmi,
                'ndvi_ndmi_corr': corr_ndvi_ndmi,
                'ndvi_ndmi_pval': p_ndvi_ndmi,
            }
        except:
            # Handle constant values or other errors
            correlation_results[(i, j)] = {
                'lstf_ndvi_corr': np.nan,
                'lstf_ndvi_pval': np.nan,
                'lstf_ndmi_corr': np.nan,
                'lstf_ndmi_pval': np.nan,
                'ndvi_ndmi_corr': np.nan,
                'ndvi_ndmi_pval': np.nan,
            }
    else:
        correlation_results[(i, j)] = {
            'lstf_ndvi_corr': np.nan,
            'lstf_ndvi_pval': np.nan,
            'lstf_ndmi_corr': np.nan,
            'lstf_ndmi_pval': np.nan,
            'ndvi_ndmi_corr': np.nan,
            'ndvi_ndmi_pval': np.nan,
        }

# Calculate overall statistics across all pixels
all_lstf_ndvi = [r['lstf_ndvi_corr'] for r in correlation_results.values() if not np.isnan(r['lstf_ndvi_corr'])]
all_lstf_ndmi = [r['lstf_ndmi_corr'] for r in correlation_results.values() if not np.isnan(r['lstf_ndmi_corr'])]
all_ndvi_ndmi = [r['ndvi_ndmi_corr'] for r in correlation_results.values() if not np.isnan(r['ndvi_ndmi_corr'])]

'''print("\n=== Correlations with Pixel Coordinates ===")
for (i, j), result in correlation_results.items():
    if not np.isnan(result['lstf_ndvi_corr']):
        pr_lstfNdvi = round((result['lstf_ndvi_corr']), 3)
        pr_lstfNdmi = round((result['lstf_ndmi_corr']), 3)
        pr_ndviNdmi = round((result['ndvi_ndmi_corr']), 3)
        
        print(
            f"Pixel ({i},{j}): LSTF-NDVI={result['lstf_ndvi_corr']:.3f}, "
              f"LSTF-NDMI={result['lstf_ndmi_corr']:.3f}, "
              f"NDVI-NDMI={result['ndvi_ndmi_corr']:.3f}")'''
        
'''print("\n=== Overall Correlation Statistics ===")
print(f"LSTF vs NDVI: mean={np.mean(all_lstf_ndvi):.3f}, median={np.median(all_lstf_ndvi):.3f}")
print(f"LSTF vs NDMI: mean={np.mean(all_lstf_ndmi):.3f}, median={np.median(all_lstf_ndmi):.3f}")
print(f"NDVI vs NDMI: mean={np.mean(all_ndvi_ndmi):.3f}, median={np.median(all_ndvi_ndmi):.3f}")'''

coords_path = os.path.join(data_dir, f"geo_{locationKey}{suffix}")
coords_arr = np.load(coords_path, allow_pickle=True)
print("coords_arr shape: ", coords_arr.shape)
######################################################################################
# Clear existing entries in spectral_temporal table
cursor.execute(f"DELETE FROM spectral_temporal")
conn.commit()
######################################################################################
deciRound = 1
with open(
    os.path.join(data_dir, f"ls8_{locationKey}_temporal.csv"), 
    mode='w', newline='', encoding='utf-8') as file:

    writer = csv.writer(file)

    #writer.writerow(list(temporalComposite.keys()))
    writer.writerow([
        'lat', 'lon', 
        'lstf', 'lstf_serc', 'lstf_arc',
        'ndvi', 'ndvi_serc', 'ndvi_arc', 
        'ndmi', 'ndmi_serc', 'ndmi_arc',
        'idx_row','idx_col',
        'lstf_ndvi_corr','lstf_ndmi_corr', 'ndvi_ndmi_corr',
        'lstf_ndvi_pval', 'lstf_ndmi_pval', 'ndvi_ndmi_pval'
    ])
    rowId = 0
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
                lstf_serc = analysisStack["lstf_serc"][i, j]
                #lstf_serc = list(map(lambda x: round((x), deciRound), lstf_serc))
                # lstf mean year over year change
                lstf_arc = analysisStack["lstf_arc"][i, j]
                #lstf_arc = list(map(lambda x: round((x), deciRound), lstf_arc))
                # ndvi difference
                ndvi_diff = analysisStack["ndvi_diff"][:, i, j].tolist()
                ndvi_diff = list(map(lambda x: round((x), deciRound), ndvi_diff))
                # ndvi overall change
                ndvi_serc = analysisStack["ndvi_serc"][i, j].tolist()
                #ndvi_serc = list(map(lambda x: round((x), deciRound), ndvi_serc))
                # ndvi mean year over year change
                ndvi_arc = analysisStack["ndvi_arc"][i, j].tolist()
                #ndvi_arc = list(map(lambda x: round((x), deciRound), ndvi_arc))

                ndmi = analysisStack["ndmi_data"][:, i, j].tolist()
                ndmi = np.mean(list(map(lambda x: round((x), deciRound), ndmi)))
                ndmi_diff = analysisStack["ndmi_diff"][:, i, j].tolist()
                ndmi_serc = analysisStack["ndmi_serc"][i, j].tolist()
                ndmi_arc = analysisStack["ndmi_arc"][i, j].tolist()

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

                corr_data = correlation_results.get((i, j), {})

                prc_lstfNdvi = round(corr_data.get('lstf_ndvi_corr', np.nan), 3)
                prc_lstfNdmi = round(corr_data.get('lstf_ndmi_corr', np.nan), 3)
                prc_ndviNdmi = round(corr_data.get('ndvi_ndmi_corr', np.nan), 3)
                pval_lstfNdvi = round(corr_data.get('lstf_ndvi_pval', np.nan), 3)
                pval_lstfNdmi = round(corr_data.get('lstf_ndmi_pval', np.nan), 3)
                pval_ndviNdmi = round(corr_data.get('ndvi_ndmi_pval', np.nan), 3)


                cursor.execute(
                    f'''INSERT INTO spectral_temporal (
                    'geoid', 'lat', 'lon', 
                    'lstf', 'lstf_serc',  'lstf_arc',
                    'ndvi',  'ndvi_serc',  'ndvi_arc', 
                    'ndmi', 'ndmi_serc', 'ndmi_arc' ,
                    'idx_row', 'idx_col',
                    'lstf_ndvi_corr', 'lstf_ndmi_corr',  'ndvi_ndmi_corr',
                    'lstf_ndvi_pval',  'lstf_ndmi_pval', 'ndvi_ndmi_pval'
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (   rowId,
                        lat, lon, 
                        to_py_type(round((lstf),2)), 
                        to_py_type(round((lstf_serc),2)), to_py_type(round((lstf_arc),2)), 
                        to_py_type(round((ndvi),2)), to_py_type(round((ndvi_serc),2)), to_py_type(round((ndvi_arc),2)),
                        to_py_type(round((ndmi),2)), to_py_type(round((ndmi_serc),2)), to_py_type(round((ndmi_arc),2)),
                        i, j,
                        to_py_type(prc_lstfNdvi), to_py_type(prc_lstfNdmi), to_py_type(prc_ndviNdmi),
                        to_py_type(pval_lstfNdvi), to_py_type(pval_lstfNdmi), to_py_type(pval_ndviNdmi)
                    ))
                rowId+=1
                
                writer.writerow([
                    rowId,
                    lat, lon, 
                    round((lstf),2), round((lstf_serc),2), round((lstf_arc),2), 
                    round((ndvi),2), round((ndvi_serc),2), round((ndvi_arc),2),
                    round((ndmi),2), round((ndmi_serc),2), round((ndmi_arc),2),
                    prc_lstfNdvi, prc_lstfNdmi, prc_ndviNdmi,
                    pval_lstfNdvi, pval_lstfNdmi, pval_ndviNdmi
                ])
conn.commit()
conn.close()
print("ls8Temporal.py DONE")