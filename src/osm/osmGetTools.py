import os
import json
import time
from typing import Dict, Tuple
import overpy
######################################################################################
osmApi = overpy.Overpass()
Coord = Tuple[float, float]  # (lon, lat)
######################################################################################
def get_osm_overpy(
    locationId: str,
    coordinates: Coord,
    searchRadius: float,
    osmGeneralQueryCategories: list,
    parent_dir: str,
):
    queryKey = None
    radius = int(searchRadius * 1609.34)
    lon, lat = float(coordinates[1]), float(coordinates[0])

    # Solo highways por ahora; ajusta si quieres otras categorías
    overpassCategories = {
        "highway": {"queries": [None], "geometry": ["LineString"]},
        "building": {"queries": [None], "geometry": ["Polygon"]},
        "amenity": {"queries": [None], "geometry": ["Polygon", "Point"]},
        "leisure": {"queries": [None], "geometry": ["Polygon", "Point"]},
        "shop": {"queries": [None], "geometry": ["Polygon", "Point"]},
    }

    for osmGeneralQueryCategory in osmGeneralQueryCategories:
        filterGeometry = overpassCategories[osmGeneralQueryCategory]["geometry"]

        if queryKey is not None:
            query = f"""
                [out:json][timeout:90];
                (
                  node["{osmGeneralQueryCategory}"="{queryKey}"](around:{radius}, {lat}, {lon});
                  way ["{osmGeneralQueryCategory}"="{queryKey}"](around:{radius}, {lat}, {lon});
                  relation["{osmGeneralQueryCategory}"="{queryKey}"](around:{radius}, {lat}, {lon});
                );
                out body;
                >;
                out skel qt;
            """
        elif queryKey is None and osmGeneralQueryCategory != "highway":
            query = f"""
                [out:json][timeout:90];
                (
                  node["{osmGeneralQueryCategory}"](around:{radius}, {lat}, {lon});
                  way ["{osmGeneralQueryCategory}"](around:{radius}, {lat}, {lon});
                  relation["{osmGeneralQueryCategory}"](around:{radius}, {lat}, {lon});
                );
                out body;
                >;
                out skel qt;
            """
        else:
            # HIGHWAY (drivable) — solo WAY + out body + recurse para que Overpy tenga way.nodes
            highway_regex = ("^(motorway|motorway_link|trunk|trunk_link|primary|primary_link|"
                             "secondary|secondary_link|tertiary|tertiary_link|unclassified|"
                             "residential|service)$")
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
        # ----------------------------
        # ----------------------------
        result: overpy.Result = osmApi.query(query)
        # ----------------------------
        # Convertir a GeoJSON (Overpy -> GeoJSON)
        # ----------------------------
        def overpass_to_geojson(overpy_result: overpy.Result) -> Dict:
            features = []

            # Nodes -> Points (nota: para highway no estamos pidiendo nodes, así evitamos bus_stops)
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

            # Ways -> LineString o Polygon (si está cerrado)
            for way in overpy_result.ways:
                try:
                    coords = [[float(n.lon), float(n.lat)] for n in way.nodes]
                    if len(coords) < 2:
                        continue

                    is_closed = len(coords) >= 4 and coords[0] == coords[-1]
                    if is_closed:
                        geom_type = "Polygon"
                        geometry = {"type": "Polygon", "coordinates": [coords]}
                    else:
                        geom_type = "LineString"
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

            # (Opcional) Relations si las necesitas
            return {"type": "FeatureCollection", "features": features}

        osmData_geojson = overpass_to_geojson(result)

        # ----------------------------
        # Filtrar por geometría y etiquetas especiales
        # ----------------------------
        osmData_filtered = {"type": "FeatureCollection", "features": []}

        for feature in osmData_geojson["features"]:
            geometry_type = feature["geometry"]["type"]
            properties = feature.get("properties", {})

            # Para highways, evita polígonos (plazas peatonales, etc.)
            if osmGeneralQueryCategory == "highway" and geometry_type == "Polygon":
                continue

            '''if (
                geometry_type in filterGeometry
                or any(k in properties for k in ["terminal", "concourse"])
            ):'''
            if geometry_type in filterGeometry:
                osmData_filtered["features"].append(feature)

        searchKey = (
            f"{osmGeneralQueryCategory}-{queryKey}"
            if queryKey
            else osmGeneralQueryCategory
        )
        outputFileName = f"{locationId}_{searchKey}.geojson"
        outputPath = os.path.join(parent_dir, "data", "osm", osmGeneralQueryCategory, outputFileName)
        with open(outputPath, "w", encoding="utf-8") as output_json:
            output_json.write(json.dumps(osmData_filtered, ensure_ascii=False))

        print(f"Wrote {outputPath}")
        time.sleep(8)  # sé amable con Overpass

    print("DONE")