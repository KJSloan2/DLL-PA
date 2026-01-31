#region Usings
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Rhino.Geometry;
using Grasshopper;
using Grasshopper.Kernel;
using System.Collections;
#endregion

public class Script_Instance : GH_ScriptInstance
{
  private static readonly CultureInfo INV = CultureInfo.InvariantCulture;

  // WGS-84 meters per degree (high-accuracy), then convert to feet
  private static void FeetPerDegree(double latDeg, out double feetPerDegLon, out double feetPerDegLat)
  {
    double phi = Math.PI * latDeg / 180.0;
    double m_per_deg_lat = 111132.92 - 559.82 * Math.Cos(2 * phi) + 1.175 * Math.Cos(4 * phi);
    double m_per_deg_lon = 111412.84 * Math.Cos(phi) - 93.5 * Math.Cos(3 * phi) + 0.118 * Math.Cos(5 * phi);
    const double FT_PER_M = 3.280839895;
    feetPerDegLat = m_per_deg_lat * FT_PER_M;
    feetPerDegLon = m_per_deg_lon * FT_PER_M;
  }

  private void RunScript(
		object x,
		object y,
		List<object> zz,
		List<object> za,
		ref object a,
		ref object b,
		ref object c,
		ref object d,
		ref object e,
		ref object f,
		ref object g,
		ref object h,
		ref object i,
		ref object j,
		ref object k,
		ref object l)
  {
    string fileName = x as string;

    double scaleX = 1.0, scaleY = 1.0;

    if (zz is IList list && list.Count >= 2)                    // covers double[], float[], List<double>, List<float>, object[]
    {
        scaleX = Convert.ToDouble(list[0], INV);
        scaleY = Convert.ToDouble(list[1], INV);
    };

    double adjX = 1.0, adjY = 1.0;
    if (za is IList zaList && zaList.Count >= 2)                    // covers double[], float[], List<double>, List<float>, object[]
    {
        adjX = Convert.ToDouble(zaList[0], INV);
        adjY = Convert.ToDouble(zaList[1], INV);
    };

    bool run = (bool) y;

    var points = new List<Point3d>();
    var lstf_serc = new List<float>();
    var lstf_flag = new List<string>();
    var elv_rel = new List<float>();
    var lstf_mean = new List<float>();
    var ndvi_mean = new List<float>();
    var ndvi_flag = new List<string>();
    var ndmi_mean = new List<float>();
    var idx_row = new List<int>();
    var idx_col = new List<int>();

    var dom_dir_elv = new List<float>();
    var dom_dir = new List<string>();

    if (!run || string.IsNullOrEmpty(fileName))
    { a=points; b=lstf_serc; c=lstf_flag; 
    d=elv_rel; e=lstf_mean; f=ndvi_mean; 
    g=ndvi_flag; h=ndmi_mean; i=idx_row; 
    j=idx_col; k=dom_dir; l=dom_dir_elv; return; }

    //string dirPath = @"C:\Users\Kjslo\Documents\local_dev\dynamic_lands_lab\frontend\public\terrain\";
    string dirPath = @"C:\Users\Kjslo\Documents\local_dev\dll_p\output\DustUpsRanch\aoi_filtered";

    string fullPath = Path.Combine(dirPath, fileName);

    if (!File.Exists(fullPath))
    {
      Component.AddRuntimeMessage(GH_RuntimeMessageLevel.Error, "File not found: " + fullPath);
      a=points; b=lstf_serc; c=lstf_flag; 
      d=elv_rel; e=lstf_mean; f=ndvi_mean; 
      g=ndvi_flag; h=ndmi_mean; i=idx_row; 
      j=idx_col; k=dom_dir; l=dom_dir_elv; return;
    }

    // Toggle if your altitude column is meters
    const bool ALT_IS_METERS = true;
    const double FT_PER_M = 3.280839895;

    try
    {
      // Pass 1: read raw rows and store lon/lat/altFeet
      var rawPoints = new List<Point3d>();
      double latSum = 0.0;
      int latCount  = 0;

      using (var reader = new StreamReader(fullPath))
      {
        // Skip header
        var headerLine = reader.ReadLine();

        string line;
        while ((line = reader.ReadLine()) != null)
        {
          if (string.IsNullOrWhiteSpace(line)) continue;
          var tokens = line.Split(',');
          if (tokens.Length < 18) continue;

          if (!double.TryParse(tokens[3], NumberStyles.Float, INV, out double lstfVal))  continue;
          if (!double.TryParse(tokens[4], NumberStyles.Float, INV, out double lstfSerc)) continue;
          string lstfFlag = tokens[6];

          if (!double.TryParse(tokens[7], NumberStyles.Float, INV, out double ndviVal)) continue;
          string ndviFlag = tokens[10];

          if (!double.TryParse(tokens[11], NumberStyles.Float, INV, out double ndmiVal)) continue;

          if (!double.TryParse(tokens[1], NumberStyles.Float, INV, out double lat)) continue;
          if (!double.TryParse(tokens[2], NumberStyles.Float, INV, out double lon)) continue;

          // elv_rel column (tokens[15]); keep in feet (convert if meters)
          if (!double.TryParse(tokens[15], NumberStyles.Float,   INV, out double elvRel)) continue;
          
          double altFeet = ALT_IS_METERS ? elvRel * FT_PER_M : elvRel;
          //double altFeet = elvRel;

          // Build raw point (X=lon, Y=lat, Z=feet)
          //rawPoints.Add(new Point3d(lon+adjX, lat-adjY, altFeet));
          rawPoints.Add(new Point3d(lon, lat, altFeet));
          latSum += lat; latCount++;

          // Accumulate attributes (preserve order)
          lstf_serc.Add((float)lstfSerc);
          lstf_flag.Add(lstfFlag);
          ndvi_flag.Add(ndviFlag);
          elv_rel.Add((float)elvRel);
          lstf_mean.Add((float)lstfVal);
          ndvi_mean.Add((float)ndviVal);
          ndmi_mean.Add((float)ndmiVal);

          if (!int.TryParse(tokens[16], NumberStyles.Integer, INV, out int rIndex)) rIndex = 0;
          if (!int.TryParse(tokens[17], NumberStyles.Integer, INV, out int cIndex)) cIndex = 0;
          idx_row.Add(rIndex);
          idx_col.Add(cIndex);
          
          string domDir = tokens.Length > 47 ? tokens[47] : "";
          dom_dir.Add(domDir);

          float domDirElvVal = 0.0f;
          if (tokens.Length > 50)
          {
            float.TryParse(tokens[50], NumberStyles.Float, INV, out domDirElvVal);
          }
          dom_dir_elv.Add(domDirElvVal);
        }
      }

      // Pass 2: non-uniform scaling (degrees -> feet), no translation (matches Python)
      if (rawPoints.Count > 0)
      {
        double refLat = (latCount > 0) ? (latSum / latCount) : rawPoints[0].Y;

        // feet per degree at reference latitude
        FeetPerDegree(refLat, out double feetPerDegLon, out double feetPerDegLat);

        // Non-uniform scale about WorldXY (Z left as-is)
        //Transform scaleXY = Transform.Scale(Plane.WorldXY, feetPerDegLon, feetPerDegLat, 1.0);
        Transform scaleXY = Transform.Scale(Plane.WorldXY, scaleX, scaleY, 1.0);

        foreach (var pt in rawPoints)
        {
          var p = pt;        // struct copy
          p.Transform(scaleXY);
          points.Add(p);
        }
      }
    }
    catch (Exception ex)
    {
      Component.AddRuntimeMessage(GH_RuntimeMessageLevel.Error, "Error reading CSV: " + ex.Message);
    }

    // Outputs
    a = points;
    b = lstf_serc;
    c = lstf_flag;
    d = elv_rel;
    e = lstf_mean;
    f = ndvi_mean;
    g = ndvi_flag;
    h = ndmi_mean;
    i = idx_row;
    j = idx_col;
    k = dom_dir;
    l = dom_dir_elv;
  }
}
