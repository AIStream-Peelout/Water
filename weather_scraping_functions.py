from datetime import datetime, timedelta
import math
import requests
import pandas as pd
import pytz
import json

ASOS_BASE_URL = ("https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station={}&data=tmpf&data=dwpf"
                 "&data=relh&data=feel&data=sknt&data=sped&data=alti&data=mslp&data=drct"
                 "&data=ice_accretion_1hr&data=p01m&data=vsby&data=gust&data=skyc1&data=peak_wind_gust"
                 "&data=snowdepth&year1={}&month1={}&day1={}&year2={}&month2={}&day2={}&tz=Etc%2FUTC"
                 "&format=onlycomma&latlon=no&elev=no&missing=M&trace=T&direct=no"
                 "&report_type=3&report_type=4")

# FIPS state codes (as returned by the NWIS site service) to postal abbreviations.
FIPS_TO_STATE = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO", "09": "CT", "10": "DE",
    "11": "DC", "12": "FL", "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN", "19": "IA",
    "20": "KS", "21": "KY", "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
    "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH", "34": "NJ", "35": "NM",
    "36": "NY", "37": "NC", "38": "ND", "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
    "54": "WV", "55": "WI", "56": "WY", "72": "PR",
}


def get_asos_data_from_url(station_id, base_url, start_time, end_time, station={}, stations_explored={}):
    """
    end_time: End date should always be plus one of the date scraped by the USGS function.
    """
    # TODO change URL to get non ASOS gages
    if "saved_complete" not in stations_explored:
        stations_explored["saved_complete"] = {}
    print("Getting request from ASOS")
    print(base_url.format(station_id, start_time.year, start_time.month, start_time.day, end_time.year, end_time.month, end_time.day))
    response = requests.get(base_url.format(station_id, start_time.year, start_time.month, start_time.day, end_time.year, end_time.month, end_time.day))
    with open("temp_weather_data.csv", "w+") as f:
        f.write(response.text)
    df, missing_precip, missing_temp = process_asos_csv("temp_weather_data.csv")
    station["missing_precip"] = missing_precip
    station["missing_temp"] = missing_temp
    stations_explored["saved_complete"][station_id] = station
    df.to_csv(str(station_id) + ".csv")
    return str(station_id) + ".csv"
    # name = str(station["station_id"])+".csv"
    # upload_file("predict_cfs",  "asos_new/" + name, name, client)
    # station_meta_dict[station["station_id"]] = station
    # stations_list.append(station)


def process_asos_csv(path: str):
    df = pd.read_csv(path)  # , parse_dates=['valid']
    if df.empty:
        return pd.DataFrame(), 0, 0
    print(df)
    missing_precip = df['p01m'][df['p01m']=='M'].count()
    missing_temp = df['tmpf'][df['tmpf']=='M'].count()
    df['hour_updated'] = df['valid'].map(format_dt)
    df['tmpf'] = pd.to_numeric(df['tmpf'], errors='coerce')
    df['dwpf'] = pd.to_numeric(df['dwpf'], errors='coerce')
    df['p01m'] = pd.to_numeric(df['p01m'], errors='coerce')
    # feel
    df["feel"] = pd.to_numeric(df["feel"], errors="coerce")
    df['relh'] = pd.to_numeric(df['relh'], errors='coerce')
    df['sknt'] = pd.to_numeric(df['sknt'], errors='coerce')
    df['sped'] = pd.to_numeric(df['sped'], errors='coerce')
    df['alti'] = pd.to_numeric(df['alti'], errors='coerce')
    df['gust'] = pd.to_numeric(df['gust'], errors='coerce')
    df['mslp'] = pd.to_numeric(df['mslp'], errors='coerce')
    df['vsby'] = pd.to_numeric(df['vsby'], errors='coerce')
    df['peak_wind_gust'] = pd.to_numeric(df['peak_wind_gust'], errors='coerce')
    df['snowdepth'] = pd.to_numeric(df['snowdepth'], errors='coerce')
    df['ice_accretion_1hr'] = pd.to_numeric(df['ice_accretion_1hr'], errors='coerce')
    df['drct'] = pd.to_numeric(df['drct'], errors='coerce')
    df['skyc1'] = df['skyc1'].astype(str)
    # Replace mising values with an average of the two closest values
    # Since stations record at different intervals this could
    # actually cause an overestimation of precip. Instead replace with 0
    # df['p01m']=(df['p01m'].fillna(method='ffill') + df['p01m'].fillna(method='bfill'))/2
    # df['p01m'] = df['p01m'].fillna(0)
    # df['tmpf']=(df['tmpf'].fillna(method='ffill') + df['tmpf'].fillna(method='bfill'))/2
    df = df.groupby(by=['hour_updated'], as_index=False).agg({'p01m': 'sum', 'valid': 'first', 'tmpf': 'mean', 'dwpf':'mean', 'ice_accretion_1hr':'first', 'mslp':'first', 'drct':'first', 'sped': 'first', 'alti': 'first', 'relh': 'first', 'sknt': 'first', 'feel': 'first', 'vsby': 'first', 'gust': 'first', 'skyc1': 'first', 'peak_wind_gust': 'first', 'snowdepth': 'first'})
    print("after")
    return df, int(missing_precip), int(missing_temp)


def format_dt(date_time_str: str) -> datetime:
    proper_datetime = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M")
    if proper_datetime.minute != 0:
        proper_datetime = proper_datetime + timedelta(hours=1)
        proper_datetime = proper_datetime.replace(minute=0)
    return proper_datetime


def get_asos_stations(state_abbrev: str, network_type: str = "ASOS") -> pd.DataFrame:
    """
    Lists the stations of a state's ASOS network from the Iowa Mesonet geojson service.

    :param state_abbrev: The two-letter state abbreviation, e.g. "CO".
    :type state_abbrev: str
    :param network_type: The Mesonet network suffix, defaults to "ASOS".
    :type network_type: str, optional
    :return: A dataframe with station_id, name, latitude, longitude, elevation and archive_begin columns.
    :rtype: pd.DataFrame
    """
    url = "https://mesonet.agron.iastate.edu/geojson/network/{}_{}.geojson".format(state_abbrev,
                                                                                  network_type)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    records = []
    for feature in response.json()["features"]:
        lon, lat = feature["geometry"]["coordinates"][:2]
        records.append({"station_id": feature["properties"]["sid"],
                        "name": feature["properties"]["sname"],
                        "latitude": lat, "longitude": lon,
                        "elevation": feature["properties"].get("elevation"),
                        "archive_begin": feature["properties"].get("archive_begin"),
                        "online": feature["properties"].get("online")})
    return pd.DataFrame(records)


def find_nearest_asos_station(latitude: float, longitude: float, state_abbrev: str) -> dict:
    """
    Finds the ASOS station closest to a point (e.g. a USGS gauge).

    :param latitude: The latitude of the point in decimal degrees.
    :type latitude: float
    :param longitude: The longitude of the point in decimal degrees.
    :type longitude: float
    :param state_abbrev: The two-letter state abbreviation to search in, e.g. "CO".
    :type state_abbrev: str
    :return: The station record of the nearest station with an added "distance_km" key.
    :rtype: dict
    """
    stations = get_asos_stations(state_abbrev)
    lat1, lon1 = math.radians(latitude), math.radians(longitude)
    lat2 = stations["latitude"].map(math.radians)
    lon2 = stations["longitude"].map(math.radians)
    hav = ((lat2 - lat1) / 2).map(math.sin) ** 2 + math.cos(lat1) * lat2.map(math.cos) * \
        ((lon2 - lon1) / 2).map(math.sin) ** 2
    distance_km = 2 * 6371.0 * hav.map(math.sqrt).map(math.asin)
    nearest = stations.iloc[distance_km.idxmin()].to_dict()
    nearest["distance_km"] = distance_km.min()
    return nearest


def get_hourly_asos(station_id: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
    """
    Fetches hourly ASOS surface observations for a station as a tz-aware UTC dataframe.

    A thin wrapper around :func:`get_asos_data_from_url` / :func:`process_asos_csv` (the request is made
    in UTC, so the aggregated "hour_updated" timestamps are localized to UTC) that returns a dataframe
    ready to merge on a "datetime" column.

    :param station_id: The ASOS station id, e.g. "FNL".
    :type station_id: str
    :param start_time: The start of the requested period (UTC).
    :type start_time: datetime
    :param end_time: The end of the requested period (UTC).
    :type end_time: datetime
    :return: An hourly dataframe with a tz-aware UTC "datetime" column and the ASOS measurement columns
        (tmpf, dwpf, relh, p01m, sknt, gust, snowdepth, ...).
    :rtype: pd.DataFrame
    """
    csv_path = get_asos_data_from_url(station_id, ASOS_BASE_URL, start_time,
                                      end_time + timedelta(days=1))
    df, _, _ = process_asos_csv(csv_path)
    if df.empty:
        return pd.DataFrame(columns=["datetime"])
    df["datetime"] = pd.to_datetime(df["hour_updated"]).dt.tz_localize("UTC")
    df = df.drop(columns=["hour_updated", "valid"])
    return df[(df["datetime"] >= pd.Timestamp(start_time, tz="UTC")) &
              (df["datetime"] <= pd.Timestamp(end_time, tz="UTC"))].reset_index(drop=True)


def get_snotel_data(start_time, end_time, station_id) -> pd.DataFrame:
    """A function to get the SNOTEL data from the Powderlines API.

    :param start_time: The start_time should be a datetime object.
    :type start_time: datetime.datetime
    :param end_time: The end_time should be a datetime object.
    :type end_tmime: datetime.datetime
    :param station_id: The station id should be a triplet (e.g. 427:MT:SNTL) corresponding to the station id, state, and network.
    :type station_id: str
    :return: Returns a data-frame of the SNOTEL site ranging from the start_time to the end_time.
    :rtype: pd.DataFrame
    """
    base_url = "https://powderlines.kellysoftware.org/api/station/{}?start_date={}&end_date={}"
    print("The base URL for SNOTEL is below: ")
    print(base_url.format(station_id, start_time.strftime("%Y-%m-%d"), end_time.strftime("%Y-%m-%d")))
    response = requests.get(base_url.format(station_id, start_time.strftime("%Y-%m-%d"), end_time.strftime("%Y-%m-%d")))
    json_res = json.loads(response.text)
    return pd.DataFrame(json_res["data"])
