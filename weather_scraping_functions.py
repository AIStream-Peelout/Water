from datetime import datetime, timedelta
import requests
import pandas as pd
import pytz
import json


def get_asos_data_from_url(station_id, base_url, start_time, end_time, station={}, stations_explored={}):
    """
    end_time: End date should always be plus one of the date scraped by the USGS function..
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
    df.to_csv(str(station_id)+".csv")
    return str(station_id)+".csv"
    # name = str(station["station_id"])+".csv"
    # upload_file("predict_cfs",  "asos_new/" + name, name, client)
    # station_meta_dict[station["station_id"]] = station
    # stations_list.append(station)


def process_asos_csv(path: str):
    df = pd.read_csv(path) # , parse_dates=['valid']
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
    print(df)
    print("what")
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
