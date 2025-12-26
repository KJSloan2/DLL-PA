import os
import json
import math
import time
from typing import Dict, Tuple
import requests
import overpy
######################################################################################
osmApi = overpy.Overpass()
Coord = Tuple[float, float]  # (lon, lat)
######################################################################################
def get_osm_overpy(
    locationId: str,
    coordinates: Coord,
    searchRadius: float,
    osmQueryCategories: list,
    output_dir: str,
    max_retrys=int(5),
    base_delay=int(5),
):
    queryKey = None
    radius = int(searchRadius * 1609.34)
    lon, lat = float(coordinates[1]), float(coordinates[0])

    overpassCategories = {
        "highway": {
            "queries": [None],
            "geometry": ["Polygon", "MultiPolygon", "LineString", "MultiLineString"],
        },
        "building": {
            "queries": [None],
            "geometry": ["Polygon", "MultiPolygon", "LineString", "MultiLineString"],
        },
        "amenity": {
            "queries": [None],
            "geometry": [
                "Polygon",
                "MultiPolygon",
                "LineString",
                "MultiLineString",
                "Point",
            ],
        },
        "leisure": {
            "queries": [None],
            "geometry": [
                "Polygon",
                "MultiPolygon",
                "LineString",
                "MultiLineString",
                "Point",
            ],
        },
        "shop": {
            "queries": [None],
            "geometry": [
                "Polygon",
                "MultiPolygon",
                "LineString",
                "MultiLineString",
                "Point",
            ],
        },
        "construction": {
            "queries": [None],
            "geometry": ["Polygon", "MultiPolygon", "LineString", "MultiLineString"],
        },
    }

    for osmQueryCategory in osmQueryCategories:
        filterGeometry = overpassCategories[osmQueryCategory]["geometry"]

        # Build the query
        if queryKey is not None:
            query = f"""
                [out:json][timeout:90];
                (
                node["{osmQueryCategory}"="{queryKey}"](around:{radius}, {lat}, {lon});
                way ["{osmQueryCategory}"="{queryKey}"](around:{radius}, {lat}, {lon});
                relation["{osmQueryCategory}"="{queryKey}"](around:{radius}, {lat}, {lon});
                );
                out body;
                >;
                out skel qt;
            """
        elif queryKey is None and osmQueryCategory != "highway":
            query = f"""
                [out:json][timeout:90];
                (
                node["{osmQueryCategory}"](around:{radius}, {lat}, {lon});
                way ["{osmQueryCategory}"](around:{radius}, {lat}, {lon});
                relation["{osmQueryCategory}"](around:{radius}, {lat}, {lon});
                );
                out body;
                >;
                out skel qt;
            """
        else:
            highway_regex = (
                "^(motorway|motorway_link|trunk|trunk_link|primary|primary_link|"
                "secondary|secondary_link|tertiary|tertiary_link|unclassified|"
                "residential|service)$"
            )
            if queryKey is None:
                query = f"""
                    [out:json][timeout:90];
                    way
                    ["highway"~"{highway_regex}"]
                    ["area"!="yes"]
                    (around:{radius}, {lat}, {lon});
                    out body;
                    >;
                    out skel qt;
                """
            else:
                query = f"""
                    [out:json][timeout:90];
                    way
                    ["highway"="{queryKey}"]
                    ["area"!="yes"]
                    (around:{radius}, {lat}, {lon});
                    out body;
                    >;
                    out skel qt;
                """

        # RETRY LOGIC ONLY AROUND THE API CALL
        delay = base_delay
        result = None

        for attempt in range(max_retrys):
            try:
                print(
                    f"Querying OSM for {osmQueryCategory} "
                    f"(attempt {attempt + 1}/{max_retrys})..."
                )
                result = osmApi.query(query)
                break  # ✅ Success - exit retry loop

            except overpy.exception.OverpassGatewayTimeout:
                if attempt == max_retrys - 1:
                    print(f"Failed after {max_retrys} attempts (timeout)")
                    raise
                print(f"Timeout; waiting {delay} seconds before retry...")
                time.sleep(delay)
                delay *= 2

            except overpy.exception.OverpassTooManyRequests:
                if attempt == max_retrys - 1:
                    print(f"Failed after {max_retrys} attempts (rate limit)")
                    raise
                print(f"Too many requests; waiting {delay} seconds...")
                time.sleep(delay)
                delay *= 2

        # PROCESS DATA ONLY ONCE (after successful query)
        if result is None:
            print(f"Skipping {osmQueryCategory} - no data retrieved")
            continue

        # Convert to GeoJSON
        def overpass_to_geojson(overpy_result: overpy.Result) -> Dict:
            features = []

            for node in overpy_result.nodes:
                features.append(
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [float(node.lon), float(node.lat)],
                        },
                        "properties": dict(node.tags) if node.tags else {},
                    }
                )

            for way in overpy_result.ways:
                try:
                    coords = [[float(n.lon), float(n.lat)] for n in way.nodes]
                    if len(coords) < 2:
                        continue

                    is_closed = len(coords) >= 4 and coords[0] == coords[-1]
                    if is_closed:
                        geometry = {"type": "Polygon", "coordinates": [coords]}
                    else:
                        geometry = {"type": "LineString", "coordinates": coords}

                    features.append(
                        {
                            "type": "Feature",
                            "geometry": geometry,
                            "properties": dict(way.tags) if way.tags else {},
                        }
                    )
                except Exception as e:
                    print(f"Error processing way {getattr(way, 'id', '?')}: {e}")
                    continue

            return {"type": "FeatureCollection", "features": features}

        osmData_geojson = overpass_to_geojson(result)

        # Filter features
        osmData_filtered = {"type": "FeatureCollection", "features": []}

        for feature in osmData_geojson["features"]:
            geometry_type = feature["geometry"]["type"]
            properties = feature.get("properties", {})

            if osmQueryCategory == "highway" and geometry_type == "Polygon":
                continue

            if (
                geometry_type in filterGeometry
                or any(k in properties for k in ["terminal", "concourse"])
            ):
                osmData_filtered["features"].append(feature)

        # Write output file (ONCE)
        searchKey = (
            f"{osmQueryCategory}-{queryKey}" if queryKey else osmQueryCategory
        )
        outputFileName = f"{locationId}_{searchKey}_{searchRadius}.geojson"
        outputPath = os.path.join(output_dir, outputFileName)
        with open(outputPath, "w", encoding="utf-8") as output_json:
            output_json.write(json.dumps(osmData_filtered, ensure_ascii=False))

        print(
            f"✓ Wrote {outputPath} ({len(osmData_filtered['features'])} features)"
        )

    print("DONE")
######################################################################################
######################################################################################
def get_osm_overpy_bbox(
    locationId: str,
    bbox_string: str,
    osmQueryCategories: list,
    output_dir: str,
    max_retrys=int(5),
    base_delay=int(5),
):
    """
    Query OpenStreetMap using a bounding box instead of point+radius.

    Args:
        locationId: Identifier for this location (used in output filename)
        bbox_string: Bounding box string in format "south,west,north,east"
        osmQueryCategories: List of OSM categories to query (e.g., ["highway", "building"])
        output_dir: Directory to save output GeoJSON files
        max_retrys: Maximum number of retry attempts
        base_delay: Initial delay in seconds for exponential backoff
    """
    queryKey = None

    overpassCategories = {
        "highway": {
            "queries": [None],
            "geometry": ["Polygon", "MultiPolygon", "LineString", "MultiLineString"],
        },
        "building": {
            "queries": [None],
            "geometry": ["Polygon", "MultiPolygon", "LineString", "MultiLineString"],
        },
        "amenity": {
            "queries": [None],
            "geometry": [
                "Polygon",
                "MultiPolygon",
                "LineString",
                "MultiLineString",
                "Point",
            ],
        },
        "leisure": {
            "queries": [None],
            "geometry": [
                "Polygon",
                "MultiPolygon",
                "LineString",
                "MultiLineString",
                "Point",
            ],
        },
        "shop": {
            "queries": [None],
            "geometry": [
                "Polygon",
                "MultiPolygon",
                "LineString",
                "MultiLineString",
                "Point",
            ],
        },
        "construction": {
            "queries": [None],
            "geometry": ["Polygon", "MultiPolygon", "LineString", "MultiLineString"],
        },
    }

    for osmQueryCategory in osmQueryCategories:
        filterGeometry = overpassCategories[osmQueryCategory]["geometry"]

        # Build the query using bounding box
        if queryKey is not None:
            query = f"""
                [out:json][timeout:90];
                (
                node["{osmQueryCategory}"="{queryKey}"]({bbox_string});
                way ["{osmQueryCategory}"="{queryKey}"]({bbox_string});
                relation["{osmQueryCategory}"="{queryKey}"]({bbox_string});
                );
                out body;
                >;
                out skel qt;
            """
        elif queryKey is None and osmQueryCategory != "highway" and osmQueryCategory != "construction":
            query = f"""
                [out:json][timeout:90];
                (
                node["{osmQueryCategory}"]({bbox_string});
                way ["{osmQueryCategory}"]({bbox_string});
                relation["{osmQueryCategory}"]({bbox_string});
                );
                out body;
                >;
                out skel qt;
            """
        elif osmQueryCategory == "highway":
            highway_regex = (
                "^(motorway|motorway_link|trunk|trunk_link|primary|primary_link|"
                "secondary|secondary_link|tertiary|tertiary_link|unclassified|"
                "residential|service)$"
            )
            query = f"""
                [out:json][timeout:90];
                way
                ["highway"~"{highway_regex}"]
                ["area"!="yes"]
                ({bbox_string});
                out body;
                >;
                out skel qt;
            """
        elif osmQueryCategory == "construction":
            # Simplified construction query to avoid timeout
            query = f"""
                [out:json][timeout:180];
                (
                nwr["landuse"="construction"]({bbox_string});
                nwr["construction"]({bbox_string});
                nwr["building"="construction"]({bbox_string});
                nwr["highway"="construction"]({bbox_string});
                );
                out body;
                >;
                out skel qt;
            """

        # RETRY LOGIC ONLY AROUND THE API CALL
        delay = base_delay
        result = None

        for attempt in range(max_retrys):
            try:
                print(
                    f"Querying OSM for {osmQueryCategory} "
                    f"(attempt {attempt + 1}/{max_retrys})..."
                )
                result = osmApi.query(query)
                break  # Success - exit retry loop

            except overpy.exception.OverpassGatewayTimeout:
                if attempt == max_retrys - 1:
                    print(f"Failed after {max_retrys} attempts (timeout)")
                    raise
                print(f"Timeout; waiting {delay} seconds before retry...")
                time.sleep(delay)
                delay *= 2

            except overpy.exception.OverpassTooManyRequests:
                if attempt == max_retrys - 1:
                    print(f"Failed after {max_retrys} attempts (rate limit)")
                    raise
                print(f"Too many requests; waiting {delay} seconds...")
                time.sleep(delay)
                delay *= 2

            except overpy.exception.OverpassRuntimeError as e:
                if "timed out" in str(e).lower():
                    if attempt == max_retrys - 1:
                        print(f"Failed after {max_retrys} attempts (runtime timeout)")
                        # Continue to next category instead of crashing
                        result = None
                        break
                    print(f"Runtime timeout; waiting {delay} seconds before retry...")
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise

        # PROCESS DATA ONLY ONCE (after successful query)
        if result is None:
            print(f"Skipping {osmQueryCategory} - no data retrieved")
            continue

        # Convert to GeoJSON
        def overpass_to_geojson(overpy_result: overpy.Result) -> Dict:
            features = []

            for node in overpy_result.nodes:
                features.append(
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [float(node.lon), float(node.lat)],
                        },
                        "properties": dict(node.tags) if node.tags else {},
                    }
                )

            for way in overpy_result.ways:
                try:
                    coords = [[float(n.lon), float(n.lat)] for n in way.nodes]
                    if len(coords) < 2:
                        continue

                    is_closed = len(coords) >= 4 and coords[0] == coords[-1]
                    if is_closed:
                        geometry = {"type": "Polygon", "coordinates": [coords]}
                    else:
                        geometry = {"type": "LineString", "coordinates": coords}

                    features.append(
                        {
                            "type": "Feature",
                            "geometry": geometry,
                            "properties": dict(way.tags) if way.tags else {},
                        }
                    )
                except Exception as e:
                    print(f"Error processing way {getattr(way, 'id', '?')}: {e}")
                    continue

            return {"type": "FeatureCollection", "features": features}

        osmData_geojson = overpass_to_geojson(result)

        # Filter features
        osmData_filtered = {"type": "FeatureCollection", "features": []}

        for feature in osmData_geojson["features"]:
            geometry_type = feature["geometry"]["type"]

            if geometry_type in filterGeometry:
                osmData_filtered["features"].append(feature)

        # Write output file
        searchKey = (
            f"{osmQueryCategory}-{queryKey}" if queryKey else osmQueryCategory
        )
        outputFileName = f"{locationId}_{searchKey}_bbox.geojson"
        outputPath = os.path.join(output_dir, outputFileName)
        with open(outputPath, "w", encoding="utf-8") as output_json:
            output_json.write(json.dumps(osmData_filtered, ensure_ascii=False))

        print(
            f"✓ Wrote {outputPath} ({len(osmData_filtered['features'])} features)"
        )

    print("DONE")
######################################################################################
######################################################################################
def compute_bb_pts(lat, lon, distance_miles):
    """
    Create a bounding box around a given coordinate.
    Args:
        lat: Latitude in decimal degrees
        lon: Longitude in decimal degrees
        distance_miles: Half the side length of the bounding box in miles (default: 0.25 for 1/4 mile)

    Returns:
        tuple: ((sw_lat, sw_lon), (ne_lat, ne_lon)) - Southwest and Northeast corners
    """
    # Earth's radius in miles
    EARTH_RADIUS_MILES = 3959.0
    # Convert distance to radians
    distance_radians = distance_miles / EARTH_RADIUS_MILES

    # Convert latitude to radians for calculations
    lat_radians = math.radians(lat)

    # Calculate latitude offset (same for north and south)
    lat_offset = math.degrees(distance_radians)

    # Calculate longitude offset (accounts for latitude compression)
    # At higher latitudes, longitude lines are closer together
    lon_offset = math.degrees(distance_radians / math.cos(lat_radians))

    # Southwest corner (down and to the left)
    sw_lat = lat - lat_offset
    sw_lon = lon - lon_offset

    # Northeast corner (up and to the right)
    ne_lat = lat + lat_offset
    ne_lon = lon + lon_offset

    return (sw_lat, sw_lon), (ne_lat, ne_lon)


def get_bbox_string(lat, lon, distance_miles):
    """
    Get bounding box as a comma-separated string (for Overpass API).

    Returns:
        str: "south,west,north,east" format
    """
    (sw_lat, sw_lon), (ne_lat, ne_lon) = compute_bb_pts(
        lat, lon, distance_miles
    )

    # Overpass API uses: south, west, north, east
    return f"{sw_lat},{sw_lon},{ne_lat},{ne_lon}"
######################################################################################
######################################################################################
def osm_req_bbox(
   locationId: str,
    bbox_string: str,
    osmQueryCategories: list,
    output_dir: str,
    max_retrys=int(5),
    base_delay=int(5),
):
    """
    Query OpenStreetMap using a bounding box instead of point+radius.

    Args:
        locationId: Identifier for this location (used in output filename)
        bbox_string: Bounding box string in format "south,west,north,east"
        osmQueryCategories: List of OSM categories to query (e.g., ["highway", "building"])
        output_dir: Directory to save output GeoJSON files
        max_retrys: Maximum number of retry attempts
        base_delay: Initial delay in seconds for exponential backoff
    """
    queryKey = None
    overpass_url = "https://overpass-api.de/api/interpreter"

    overpassCategories = {
        "highway": {
            "queries": [None],
            "geometry": ["Polygon", "MultiPolygon", "LineString", "MultiLineString"],
        },
        "building": {
            "queries": [None],
            "geometry": ["Polygon", "MultiPolygon", "LineString", "MultiLineString"],
        },
        "amenity": {
            "queries": [None],
            "geometry": [
                "Polygon",
                "MultiPolygon",
                "LineString",
                "MultiLineString",
                "Point",
            ],
        },
        "leisure": {
            "queries": [None],
            "geometry": [
                "Polygon",
                "MultiPolygon",
                "LineString",
                "MultiLineString",
                "Point",
            ],
        },
        "shop": {
            "queries": [None],
            "geometry": [
                "Polygon",
                "MultiPolygon",
                "LineString",
                "MultiLineString",
                "Point",
            ],
        },
        "construction": {
            "queries": [None],
            "geometry": ["Polygon", "MultiPolygon", "LineString", "MultiLineString"],
        },
    }

    for osmQueryCategory in osmQueryCategories:
        filterGeometry = overpassCategories[osmQueryCategory]["geometry"]

        # Build the query using bounding box
        if queryKey is not None:
            query = f"""
                [out:json][timeout:90];
                (
                node["{osmQueryCategory}"="{queryKey}"]({bbox_string});
                way ["{osmQueryCategory}"="{queryKey}"]({bbox_string});
                relation["{osmQueryCategory}"="{queryKey}"]({bbox_string});
                );
                out body;
                >;
                out skel qt;
            """
        elif queryKey is None and osmQueryCategory != "highway" and osmQueryCategory != "construction":
            query = f"""
                [out:json][timeout:90];
                (
                node["{osmQueryCategory}"]({bbox_string});
                way ["{osmQueryCategory}"]({bbox_string});
                relation["{osmQueryCategory}"]({bbox_string});
                );
                out body;
                >;
                out skel qt;
            """
        elif osmQueryCategory == "highway":
            highway_regex = (
                "^(motorway|motorway_link|trunk|trunk_link|primary|primary_link|"
                "secondary|secondary_link|tertiary|tertiary_link|unclassified|"
                "residential|service)$"
            )
            query = f"""
                [out:json][timeout:90];
                way
                ["highway"~"{highway_regex}"]
                ["area"!="yes"]
                ({bbox_string});
                out body;
                >;
                out skel qt;
            """
        elif osmQueryCategory == "construction":
            query = f"""
                [out:json][timeout:180];
                (
                nwr["landuse"="construction"]({bbox_string});
                nwr["construction"]({bbox_string});
                nwr["building"="construction"]({bbox_string});
                nwr["highway"="construction"]({bbox_string});
                );
                out body;
                >;
                out skel qt;
            """

        # RETRY LOGIC ONLY AROUND THE API CALL
        delay = base_delay
        result = None

        for attempt in range(max_retrys):
            try:
                print(
                    f"Querying OSM for {osmQueryCategory} "
                    f"(attempt {attempt + 1}/{max_retrys})..."
                )
                response = requests.post(overpass_url, data={"data": query}, timeout=200)
                response.raise_for_status()
                result = response.json()
                break  # Success - exit retry loop

            except requests.exceptions.Timeout:
                if attempt == max_retrys - 1:
                    print(f"Failed after {max_retrys} attempts (timeout)")
                    result = None
                    break
                print(f"Timeout; waiting {delay} seconds before retry...")
                time.sleep(delay)
                delay *= 2

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Too Many Requests
                    if attempt == max_retrys - 1:
                        print(f"Failed after {max_retrys} attempts (rate limit)")
                        result = None
                        break
                    print(f"Too many requests; waiting {delay} seconds...")
                    time.sleep(delay)
                    delay *= 2
                elif e.response.status_code == 504:  # Gateway Timeout
                    if attempt == max_retrys - 1:
                        print(f"Failed after {max_retrys} attempts (gateway timeout)")
                        result = None
                        break
                    print(f"Gateway timeout; waiting {delay} seconds before retry...")
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise

            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                if "timed out" in str(e).lower() or isinstance(e, json.JSONDecodeError):
                    if attempt == max_retrys - 1:
                        print(f"Failed after {max_retrys} attempts (error: {e})")
                        result = None
                        break
                    print(f"Error: {e}; waiting {delay} seconds before retry...")
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise

        # PROCESS DATA ONLY ONCE (after successful query)
        if result is None:
            print(f"Skipping {osmQueryCategory} - no data retrieved")
            continue

        # Convert to GeoJSON
        def overpass_to_geojson(overpass_result: Dict) -> Dict:
            features = []
            elements = overpass_result.get("elements", [])
            
            # Build node lookup
            nodes_dict = {}
            for element in elements:
                if element.get("type") == "node":
                    nodes_dict[element["id"]] = {
                        "lon": element["lon"],
                        "lat": element["lat"]
                    }

            # Process elements
            for element in elements:
                if element.get("type") == "node" and "tags" in element:
                    features.append(
                        {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [float(element["lon"]), float(element["lat"])],
                            },
                            "properties": element.get("tags", {}),
                        }
                    )
                
                elif element.get("type") == "way" and "tags" in element:
                    try:
                        node_refs = element.get("nodes", [])
                        coords = []
                        for node_id in node_refs:
                            if node_id in nodes_dict:
                                node = nodes_dict[node_id]
                                coords.append([float(node["lon"]), float(node["lat"])])
                        
                        if len(coords) < 2:
                            continue

                        is_closed = len(coords) >= 4 and coords[0] == coords[-1]
                        if is_closed:
                            geometry = {"type": "Polygon", "coordinates": [coords]}
                        else:
                            geometry = {"type": "LineString", "coordinates": coords}

                        features.append(
                            {
                                "type": "Feature",
                                "geometry": geometry,
                                "properties": element.get("tags", {}),
                            }
                        )
                    except Exception as e:
                        print(f"Error processing way {element.get('id', '?')}: {e}")
                        continue

            return {"type": "FeatureCollection", "features": features}

        osmData_geojson = overpass_to_geojson(result)

        # Filter features
        osmData_filtered = {"type": "FeatureCollection", "features": []}

        for feature in osmData_geojson["features"]:
            geometry_type = feature["geometry"]["type"]

            if geometry_type in filterGeometry:
                osmData_filtered["features"].append(feature)

        # Write output file
        searchKey = (
            f"{osmQueryCategory}-{queryKey}" if queryKey else osmQueryCategory
        )
        outputFileName = f"{locationId}_{searchKey}_bbox.geojson"
        outputPath = os.path.join(output_dir, outputFileName)
        with open(outputPath, "w", encoding="utf-8") as output_json:
            output_json.write(json.dumps(osmData_filtered, ensure_ascii=False))

        print(
            f"✓ Wrote {outputPath} ({len(osmData_filtered['features'])} features)"
        )

    print("DONE")