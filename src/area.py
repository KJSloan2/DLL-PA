import math

r = 10000  # radius in feet
area_sqft = math.pi * r**2
area_sqmi = area_sqft / (5280**2)

print(area_sqmi)

ptA = [-105.89466333340697, 32.7867112397943]
ptB=[-105.8348355354846, 32.83548975972199]

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
dist = haversine(ptA, ptB)
print(dist["ml"])

squareArea = (dist["ml"]**2)
print(squareArea)



