from datetime import datetime, timedelta
import pandas as pd
import requests

def get_asos_data_from_url(station_id, start_time, end_time, station={}, stations_explored = {}):
    """_summary_
    :param station_id: _description_
    :type station_id: _type_
    :param start_time: _description_
    :type start_time: _type_
    :param end_time: End date should always be plus one of the date scraped by the USGS function.
    :type end_time: _type_
    :param station: _description_, defaults to {}
    :type station: dict, optional
    :param stations_explored: _description_, defaults to {}
    :type stations_explored: dict, optional
    """
    base_url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station={}&data=tmpf&data=p01m&data=dwpf&data=relh&data=feel&data=drct&data=sped&data=mslp&data=ice_accretion_1hr&year1={}&month1={}&day1={}&year2={}&month2={}&day2={}&tz=Etc%2FUTC&format=onlycomma&latlon=no&missing=M&trace=T&direct=no&report_type=1&report_type=2"
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
