import json 

def make_gage_data_csv(file_path:str):
  with open(file_path) as f: 
        df = pd.read_json(f)
        df = df.T
        #columns = ["index"]
        #print(df.columns)
        #columns += list(df.columns)
        #df.columns = columns 
        #df.index.name = "id"
  with open(file_path) as j:
    json_data = json.load(j)
    df["full_id"] = list(json_data.keys())
  return df

from math import radians, cos, sin, asin, sqrt

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def make_distance_file(gage_df, station_df, row_num):
  gage_data = json.loads(gage_df.iloc[row_num].to_json())
  sort_dis = [99999, 99999, 99999, 99999, 99999, 99999, 99999, 99999, 99999, 99999, 99999]
  stations = []
  for id, station in station_df.iterrows():
    distance = haversine(gage_data["logitude"], gage_data["latitude"], station["lon"], station["lat"])
    sort_dis = sorted(sort_dis)
    if distance < sort_dis[10]:
      stations.append({
          "station_id": station["stid"],
          "station_json":station.to_json(),
          "distance":distance
      })
      sort_dis.pop()
      sort_dis.append(distance)
  gage_data["stations"] = sorted(stations, key = lambda x: x["distance"], reverse=False)
  return gage_data
