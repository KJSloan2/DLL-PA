import math
def geodetic_to_ecef(lat, lon, h):
	"""Convert geodetic coordinates to ECEF.
	Parameters: lat -- Latitude in degrees; lon -- Longitude in degrees; h -- Elevation in meters
	Returns: x, y, z -- ECEF coordinates in meters"""
	a = 6378137.0  # WGS-84 Earth semimajor axis (meters)
	f = 1 / 298.257223563  # WGS-84 flattening factor
	e2 = 2 * f - f ** 2  # Square of eccentricity
	# Convert latitude and longitude from degrees to radians
	lat_rad = math.radians(lat)
	lon_rad = math.radians(lon)
	# Calculate prime vertical radius of curvature
	N = a / math.sqrt(1 - e2 * math.sin(lat_rad) ** 2)
	# Calculate ECEF coordinates
	x = (N + h) * math.cos(lat_rad) * math.cos(lon_rad)
	y = (N + h) * math.cos(lat_rad) * math.sin(lon_rad)
	z = (N * (1 - e2) + h) * math.sin(lat_rad)
	return x, y, z
######################################################################################