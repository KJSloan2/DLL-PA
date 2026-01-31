import math, json
import Rhino.Geometry as rg

def haversine(pt1, pt2):
    R = 6371000.0  # meters
    lat1, lon1 = pt1[1], pt1[0]
    lat2, lon2 = pt2[1], pt2[0]
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ = math.radians(lat2 - lat1)
    dλ = math.radians(lon2 - lon1)
    a = math.sin(dφ/2)**2 + math.cos(φ1)*math.cos(φ2)*math.sin(dλ/2)**2
    c = 2*math.atan2(math.sqrt(a), math.sqrt(1-a))
    m = R*c
    ft = m * 3.28084
    return {"m": m, "ft": ft}
if x:
    # ---- load bbox: [[minLon,minLat], [maxLon,maxLat]]
    log_path = r"\log.json"
    with open(log_path, "r", encoding="utf-8") as f:
        logJson = json.load(f)

    bb = logJson["ls8_bounds"]["2014"]["bb"]
    minLon, minLat = bb[0]
    maxLon, maxLat = bb[1]
    latC = 0.5*(minLat + maxLat)   # center latitude (for EW distance)

    # Degree spans
    dx_deg = abs(maxLon - minLon)
    dy_deg = abs(maxLat - minLat)

    # Feet across one degree in X and Y, measured along bbox edges
    # X (east-west): keep latitude ~ constant
    ew_ft = haversine([minLon, latC], [maxLon, latC])["ft"] if dx_deg else 0.0
    # Y (north-south): keep longitude constant
    ns_ft = haversine([minLon, minLat], [minLon, maxLat])["ft"] if dy_deg else 0.0

    scaleX = ew_ft / dx_deg if dx_deg else 0.0  # feet per degree of longitude
    scaleY = ns_ft / dy_deg if dy_deg else 0.0  # feet per degree of latitude

    # ---- make points in degree space
    pt_sw = rg.Point3d(minLon, minLat, 0.0)
    pt_se = rg.Point3d(maxLon, minLat, 0.0)
    pt_ne = rg.Point3d(maxLon, maxLat, 0.0)

    #center = (minLon-maxLon)
    # Non-uniform XY scale, don't touch Z
    xform = rg.Transform.Scale(rg.Plane.WorldXY, scaleX, scaleY, 1.0)

    # IMPORTANT: Transform mutates; clone first
    sw_t = rg.Point3d(pt_sw); sw_t.Transform(xform)
    se_t = rg.Point3d(pt_se); se_t.Transform(xform)
    ne_t = rg.Point3d(pt_ne); ne_t.Transform(xform)

    # Rectangle (scaled)
    rect = rg.Rectangle3d(rg.Plane.WorldXY,
                        rg.Interval(min(pt_sw.X, pt_ne.X), max(pt_sw.X, pt_ne.X)),
                        rg.Interval(min(pt_sw.Y, pt_ne.Y), max(pt_sw.Y, pt_ne.Y)))

    rect = rg.Rectangle3d(
        rg.Plane.WorldXY,
        rg.Interval(minLon, maxLon),
        rg.Interval(minLat, maxLat)
    )

    # Copy correctly, then transform in place
    rect_t = rg.Rectangle3d(rect.Plane, rect.X, rect.Y)  #copy via plane + intervals
    rect_t.Transform(xform)

    adjXY = [se_t.X, se_t.Y]


    # Outputs (example: a,b,c,d are GH outputs)
    a = [scaleX, scaleY]   # feet per degree (X,Y)
    b = sw_t               # scaled SW point
    c = se_t               # scaled SE point
    d = ne_t               # scaled NE point
    e = adjXY