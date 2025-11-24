import fiona
import json
from shapely.geometry import Point, Polygon
import os

for f in ["charlotteNC1819", "charlotteNC1920", "charlotteNC2021", "charlotteNC2122", "charlotteNC2223"]:
    fname = f[0:-4]
    year = "20"+str(f[-2:])

    output = {
        "type": "FeatureCollection",
        "features": []
    }

    taxParcelsGeojson = r"output\Buildings_TF.geojson"
    ucaPolygonsGeojson = os.path.join(r"output\urban_change", f+".geojson")

    ucaPolygons = []
    with fiona.open(ucaPolygonsGeojson, mode="r") as src_uca:
        print("Total Features:", len(src_uca))
        print("Schema:", src_uca.schema)

        # Iterate through features
        for uca_feature in src_uca:
            geometry = uca_feature["geometry"]
            ucaPolygons.append(Polygon(geometry["coordinates"][0]))
    ######################################################################################
    with fiona.open(taxParcelsGeojson, mode="r") as src_query:
        for queryFeature in src_query:
            #-80.87880645297459, 35.19180530247534
            poolCoords = [[],[]]
            for pt in queryFeature["geometry"]["coordinates"][0]:
                poolCoords[0].append(pt[0])
                poolCoords[1].append(pt[1])
            centroid = [sum(poolCoords[0])/len(poolCoords[0]), sum(poolCoords[1])/len(poolCoords[1])]
            
            query_point = Point(centroid)
            for ucaPolygon in ucaPolygons:
                if ucaPolygon.contains(query_point):

                    properties = {}
                    for key, value in queryFeature["properties"].items():
                        properties[key] = value

                    geometry = queryFeature["geometry"]
                    newFeature = {
                        "type": "Feature",
                        "properties": properties,
                        "geometry": {
                            "type": geometry["type"],
                            "coordinates": geometry["coordinates"]
                        }   
                    }
                    newFeature["properties"]["centroid"] = centroid
                    output["features"].append(newFeature)

    with open(os.path.join(r"C:\Users\12903\OneDrive - Corgan\Desktop\Local Development\deckGis\deckgis\public\geojson\\", "uca_"+fname+year+"_structures.geojson"), "w", encoding='utf-8') as output_json:
        output_json.write(json.dumps(output, indent=1, ensure_ascii=False))
    
    print(f"{fname+year} has been processed")
print('DONE')

######################################################################################
