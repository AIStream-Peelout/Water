from datetime import datetime, timedelta
import requests
import pandas as pd
import json


def get_asos_data_from_url(station_id, base_url, start_time, end_time, station={}, stations_explored={}):
    """
    end_time: End date should always be plus one of the date scraped by the USGS function.
    """
    # TODO change URL to get non ASOS gages
    if "saved_complete" not in stations_explored:
        stations_explored["saved_complete"] = {}
    response = requests.get(base_url.format(station_id, start_time.year, start_time.month, start_time.day, end_time.year, end_time.month, end_time.day))
    with open("temp_weather_data.csv", "w+") as f:
        f.write(response.text)
    df, missing_precip, missing_temp = process_asos_csv("temp_weather_data.csv")
    station["missing_precip"] = missing_precip
    station["missing_temp"] = missing_temp
    stations_explored["saved_complete"][station_id] = station
    df.to_csv(str(station_id)+".csv")
    # Caching code
    # name = str(station["station_id"])+".csv"
    # upload_file("predict_cfs",  "asos_new/" + name, name, client)
    # station_meta_dict[station["station_id"]] = station
    # stations_list.append(station)


def process_asos_csv(path: str):
    df = pd.read_csv(path)
    print(df)
    missing_precip = df['p01m'][df['p01m']=='M'].count()
    missing_temp = df['tmpf'][df['tmpf']=='M'].count()
    df['hour_updated'] = df['valid'].map(format_dt)
    df['tmpf'] = pd.to_numeric(df['tmpf'], errors='coerce')
    df['dwpf'] = pd.to_numeric(df['dwpf'], errors='coerce')
    df['p01m'] = pd.to_numeric(df['p01m'], errors='coerce')
    print(df)
    # Replace mising values with an average of the two closest values
    # Since stations record at different intervals this could
    # actually cause an overestimation of precip. Instead replace with 0
    # df['p01m']=(df['p01m'].fillna(method='ffill') + df['p01m'].fillna(method='bfill'))/2
    # df['p01m'] = df['p01m'].fillna(0)
    # df['tmpf']=(df['tmpf'].fillna(method='ffill') + df['tmpf'].fillna(method='bfill'))/2
    df = df.groupby(by=['hour_updated'], as_index=False).agg({'p01m': 'sum', 'valid': 'first', 'tmpf': 'mean', 'dwpf':'mean', 'ice_accretion_1hr':'first', 'mslp':'first', 'drct':'first'})
    return df, int(missing_precip), int(missing_temp)


def format_dt(date_time_str: str) -> datetime:
    proper_datetime = datetime.strptime(date_time_str, "%Y-%m-%d %H:%M")
    if proper_datetime.minute != 0:
        proper_datetime = proper_datetime + timedelta(hours=1)
        proper_datetime = proper_datetime.replace(minute=0)
    return proper_datetime


def get_snotel_daily_data(station_id: int, start_date: str, end_date: str) -> pd.DataFrame:
    """Gets the SNOTEL data from the powderlines API

    ..
    get_snotel("663:CO:SNTL", ,, datetime(2023, 2, 2))
    ..

    :param station_id: id of the SNOTEL site shou
    :type station_id: int
    :param start_date: The date to start the SNOTEL scraping on (inclusive)
    :type start_date: datetime
    :param end_date: The date to end the SNOTEL scraping on (inclusive)
    :type end_date: datetime
    :return: A dataframe of the SNOTEL data
    :rtype: pd.DataFrame
    """
    base_url = "https://powderlines.kellysoftware.org/api/station/{}?start_date={}&end_date={}"
    response = requests.get(base_url.format(station_id, start_date, end_date))
    r = response.text
    json_res = json.loads(response.text)
    return pd.DataFrame(json_res["data"])


def combine_snotel_with_df(combined_df, snotel) -> pd.DataFrame:
    """Combines the SNOTEL data with the ASOS/USGS data

    :param combined_df: _description_
    :type combined_df: _type_
    :param snotel: _description_
    :type snotel: _type_
    :return: Combined data-frame
    :rtype: _type_
    """
    snotel["Date"] = pd.to_datetime(snotel["Date"], utc=True)
    combined_df = combined_df.merge(snotel, left_on="hour_updated", right_on="Date", how="left")
    combined_df["snow_depth"] = combined_df["Snow Depth (in)"].interpolate(method='nearest').ffill().bfill()
    combined_df["snow_water"] = combined_df["Snow Water Equivalent (in)"].interpolate(method='nearest').ffill().bfill()
    combined_df["change_in_snow_depth"] = combined_df["snow_depth"].diff()
    return combined_df
