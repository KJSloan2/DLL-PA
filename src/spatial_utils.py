import math
from shapely.geometry import MultiLineString, Polygon, Point, mapping
######################################################################################
def haversine(pt1, pt2):
	# Radius of the Earth in meters
	R = 6371000
	# Convert latitude and longitude from degrees to radians
	lat1, lon1 = pt1[1], pt1[0]
	lat2, lon2 = pt2[1], pt2[0]
	phi1 = math.radians(lat1)
	phi2 = math.radians(lat2)
	delta_phi = math.radians(lat2 - lat1)
	delta_lambda = math.radians(lon2 - lon1)
	# Haversine formula
	a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
	c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
	# Distance in meters
	dist_m = R * c
	# Convert meters to feet
	coef_ft = 3.28084
	dist_ft = dist_m * coef_ft
	# Convert feet to miles
	dist_ml = round(dist_ft / 5280, 2)
	return {"ft": dist_ft, "m": dist_m, "ml": dist_ml}
######################################################################################
def mpolygon_yeild_pts(geometry):
    """
    Yields all (lon, lat) tuples from a Polygon or MultiPolygon GeoJSON geometry.
    """
    geom_type = geometry["type"]

    if geom_type == "Polygon":
        for ring in geometry["coordinates"]:
            for coord in ring:
                yield tuple(coord)

    elif geom_type == "MultiPolygon":
        for polygon in geometry["coordinates"]:
            for ring in polygon:
                for coord in ring:
                    yield tuple(coord)
    else:
        raise ValueError(f"Unsupported geometry type: {geom_type}")
######################################################################################
def create_circle(center_x, center_y, radius_miles, resolution=16):
    """
    Create a circle polygon with a radius in miles, assuming lat/lon coordinates.

    Parameters:
    - center_x (float): Longitude of center
    - center_y (float): Latitude of center
    - radius_miles (float): Radius in miles
    - resolution (int): Circle smoothness (number of points)

    Returns:
    - tuple: (Shapely Polygon geometry, GeoJSON Feature as dict)
    """

    # Approximate conversion: 1 degree latitude ≈ 69 miles
    miles_per_degree = 69.0
    radius_degrees = radius_miles / miles_per_degree

    center = Point(center_x, center_y)
    circle = center.buffer(radius_degrees, resolution=resolution)

    geojson_feature = {
        "type": "Feature",
        "geometry": mapping(circle),
        "properties": {
            "radius_miles": radius_miles
        }
    }

    return circle, geojson_feature
######################################################################################
def polygon_filter(lon, lat, polygon):
    """Check if a point falls within a polygon
    
    Args:
        lon: Longitude of the point
        lat: Latitude of the point
        polygon: A Shapely Polygon object
        
    Returns:
        bool: True if point is within polygon, False otherwise
    """
    point = Point(lon, lat)
    return polygon.contains(point)