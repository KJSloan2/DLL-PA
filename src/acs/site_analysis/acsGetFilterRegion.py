from shapely.geometry import MultiLineString, Polygon, MultiPolygon, Point, shape, mapping
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
import geopandas as gpd
import json
import os
from collections import Counter
import numpy as np

######################################################################################
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import  json_serialize
######################################################################################
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))
root_dir = os.path.abspath(os.path.join(parent_dir, ".."))

gridRefGeoJson_path = os.path.join(parent_dir, "resources", "grid.geojson")
######################################################################################
def calc_prct_overlap(geom1: BaseGeometry, geom2: BaseGeometry, relative_to="first") -> float:
    """
    Calculate percent overlap between two Shapely geometries.

    Args:
        geom1 (BaseGeometry): First geometry (Polygon or MultiPolygon)
        geom2 (BaseGeometry): Second geometry (Polygon or MultiPolygon)
        relative_to (str): One of 'first', 'second', or 'union' — base for percentage

    Returns:
        float: Percent overlap (0.0 to 100.0)
    """
    if geom1.is_empty or geom2.is_empty:
        return 0.0

    intersection = geom1.intersection(geom2)
    intersection_area = intersection.area

    if intersection_area == 0:
        return 0.0

    if relative_to == "first":
        base_area = geom1.area
    elif relative_to == "second":
        base_area = geom2.area
    elif relative_to == "union":
        base_area = unary_union([geom1, geom2]).area
    else:
        raise ValueError("relative_to must be 'first', 'second', or 'union'")

    return (intersection_area / base_area) * 100.0
######################################################################################
with open(gridRefGeoJson_path, "r") as gridRefGeoJson_file:
    gridRefGeoJson = json.load(gridRefGeoJson_file)

placeGeoJson_path = os.path.join(parent_dir, "resources", "reference", "tl_2024_48_place.geojson")
with open(placeGeoJson_path, "r") as placeGeoJson_file:
    placeGeoJson = json.load(placeGeoJson_file)

for gridRefFeature in gridRefGeoJson["features"]:
    gridRefGeom = shape(gridRefFeature["geometry"])
    for placeFeature in placeGeoJson["features"]:
        placeGeom = shape(placeFeature["geometry"])
        prctOverlap = calc_prct_overlap(gridRefGeom, placeGeom, relative_to="first")
        if prctOverlap > 10:
            print(gridRefFeature["properties"]["id"], placeFeature["properties"]["NAME"], prctOverlap)

print("DONE")