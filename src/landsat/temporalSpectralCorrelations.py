import os
import numpy as np
import json

from scipy.stats import t as t_dist
import pandas as pd
import duckdb
######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from global_functions.utils import get_files, to_py_type
######################################################################################
TARGET_PERIOD = "Q1"
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))
######################################################################################
logJson = json.load(open(os.path.join(parent_dir, 'resources', "log.json")))
locationKey = logJson["location_key"]
######################################################################################
data_dir = os.path.join(parent_dir, "output", "landsat", locationKey, "arrays")
######################################################################################
conn = duckdb.connect(os.path.join(parent_dir, 'tempGeo.duckdb'))
######################################################################################
metricKeys = ["lstf", "ndvi", "ndmi"]
analysisStack = {}

for metricKey in metricKeys:
    prefix = f"{metricKey}_{locationKey}"
    ext = ".npy"
    start_year = 2013
    end_year = 2024
    years = list(range(start_year, end_year + 1))

    data_stack = []
    for year in years:
        try:
            path = os.path.join(data_dir, f"{prefix}_{year}_{TARGET_PERIOD}{ext}")
            arr = np.load(path, allow_pickle=True)
            data_stack.append(arr)
        except Exception:
            continue

    data_stack = np.stack(data_stack, axis=0)
    diff_stack = np.diff(data_stack, axis=0)
    overall_change = data_stack[-1] - data_stack[0]
    meanYoyChange = overall_change / (end_year - start_year)

    analysisStack[f"{metricKey}_data"] = data_stack
    analysisStack[f"{metricKey}_diff"] = diff_stack
    analysisStack[f"{metricKey}_serc"] = overall_change
    analysisStack[f"{metricKey}_arc"] = meanYoyChange

######################################################################################
# Vectorized Pearson correlation across all pixels at once
######################################################################################
def vectorized_pearsonr(x, y):
    """x, y: shape (T, H, W) — computes r and p-value across axis 0."""
    n = (~np.isnan(x) & ~np.isnan(y)).sum(axis=0).astype(float)
    mx = np.nanmean(x, axis=0)
    my = np.nanmean(y, axis=0)
    dx = x - mx
    dy = y - my
    cov = np.nansum(dx * dy, axis=0)
    sx = np.sqrt(np.nansum(dx**2, axis=0))
    sy = np.sqrt(np.nansum(dy**2, axis=0))
    with np.errstate(invalid='ignore', divide='ignore'):
        r = cov / (sx * sy)
        t = r * np.sqrt((n - 2) / (1 - r**2))
    p = 2 * t_dist.sf(np.abs(t), df=n - 2)
    return r, p

lstf_data = analysisStack["lstf_data"]  # (T, H, W)
ndvi_data = analysisStack["ndvi_data"]
ndmi_data = analysisStack["ndmi_data"]

print("Computing vectorized correlations...")
corr_lstf_ndvi, pval_lstf_ndvi = vectorized_pearsonr(lstf_data, ndvi_data)
corr_lstf_ndmi, pval_lstf_ndmi = vectorized_pearsonr(lstf_data, ndmi_data)
corr_ndvi_ndmi, pval_ndvi_ndmi = vectorized_pearsonr(ndvi_data, ndmi_data)

######################################################################################
# Load coordinate arrays
######################################################################################
ext = ".npy"
latArray = np.load(os.path.join(data_dir, f"lat_{locationKey}{ext}"), allow_pickle=True)
lonArray = np.load(os.path.join(data_dir, f"lon_{locationKey}{ext}"), allow_pickle=True)

print(f"latArray shape: {latArray.shape}")
print(f"lonArray shape: {lonArray.shape}")

H, W = latArray.shape

######################################################################################
# Build output DataFrame vectorized
######################################################################################
print("Building output DataFrame...")

# Valid pixel mask — skip zero-coordinate cells
valid_mask = ~((latArray == 0) & (lonArray == 0))
valid_i, valid_j = np.where(valid_mask)

n_valid = len(valid_i)
print(f"Valid pixels: {n_valid}")

deciRound = 1

lstf_mean = np.round(np.nanmean(lstf_data, axis=0), 2)
ndvi_mean = np.round(np.nanmean(ndvi_data, axis=0), 2)
ndmi_mean = np.round(np.nanmean(ndmi_data, axis=0), 2)

# Temporal strings — build per valid pixel (unavoidable string operation)
print("Building temporal strings...")
lstf_temporal_strs = []
ndvi_temporal_strs = []
ndmi_temporal_strs = []

for k in range(n_valid):
    i, j = valid_i[k], valid_j[k]
    lstf_temporal_strs.append(','.join(map(str, lstf_data[:, i, j].tolist())))
    ndvi_temporal_strs.append(','.join(map(str, ndvi_data[:, i, j].tolist())))
    ndmi_temporal_strs.append(','.join(map(str, ndmi_data[:, i, j].tolist())))

output_df = pd.DataFrame({
    'geoid':           np.arange(n_valid),
    'lat':             latArray[valid_i, valid_j],
    'lon':             lonArray[valid_i, valid_j],
    'lstf':            lstf_mean[valid_i, valid_j],
    'lstf_serc':       np.round(analysisStack["lstf_serc"][valid_i, valid_j], 2),
    'lstf_arc':        np.round(analysisStack["lstf_arc"][valid_i, valid_j], 2),
    'ndvi':            ndvi_mean[valid_i, valid_j],
    'ndvi_serc':       np.round(analysisStack["ndvi_serc"][valid_i, valid_j], 2),
    'ndvi_arc':        np.round(analysisStack["ndvi_arc"][valid_i, valid_j], 2),
    'ndmi':            ndmi_mean[valid_i, valid_j],
    'ndmi_serc':       np.round(analysisStack["ndmi_serc"][valid_i, valid_j], 2),
    'ndmi_arc':        np.round(analysisStack["ndmi_arc"][valid_i, valid_j], 2),
    'idx_row':         valid_i,
    'idx_col':         valid_j,
    'lstf_ndvi_corr':  np.round(corr_lstf_ndvi[valid_i, valid_j], 3),
    'lstf_ndmi_corr':  np.round(corr_lstf_ndmi[valid_i, valid_j], 3),
    'ndvi_ndmi_corr':  np.round(corr_ndvi_ndmi[valid_i, valid_j], 3),
    'lstf_ndvi_pval':  np.round(pval_lstf_ndvi[valid_i, valid_j], 3),
    'lstf_ndmi_pval':  np.round(pval_lstf_ndmi[valid_i, valid_j], 3),
    'ndvi_ndmi_pval':  np.round(pval_ndvi_ndmi[valid_i, valid_j], 3),
    'lstf_temporal':   lstf_temporal_strs,
    'ndvi_temporal':   ndvi_temporal_strs,
    'ndmi_temporal':   ndmi_temporal_strs,
})

######################################################################################
# Bulk insert into DuckDB
######################################################################################
print("Inserting into spectral_temporal...")
conn.execute("DELETE FROM spectral_temporal")
conn.register("spectral_temp", output_df)
conn.execute("""
    INSERT INTO spectral_temporal (
        geoid, lat, lon,
        lstf, lstf_serc, lstf_arc,
        ndvi, ndvi_serc, ndvi_arc,
        ndmi, ndmi_serc, ndmi_arc,
        idx_row, idx_col,
        lstf_ndvi_corr, lstf_ndmi_corr, ndvi_ndmi_corr,
        lstf_ndvi_pval, lstf_ndmi_pval, ndvi_ndmi_pval,
        lstf_temporal, ndvi_temporal, ndmi_temporal
    )
    SELECT
        geoid, lat, lon,
        lstf, lstf_serc, lstf_arc,
        ndvi, ndvi_serc, ndvi_arc,
        ndmi, ndmi_serc, ndmi_arc,
        idx_row, idx_col,
        lstf_ndvi_corr, lstf_ndmi_corr, ndvi_ndmi_corr,
        lstf_ndvi_pval, lstf_ndmi_pval, ndvi_ndmi_pval,
        lstf_temporal, ndvi_temporal, ndmi_temporal
    FROM spectral_temp
""")
conn.commit()
conn.close()
print("temporalSpectralCorrelations.py DONE")