"""
Functions for scraping the USDA NRCS Air-Water Database (AWDB) REST API.

The AWDB REST API (https://wcc.sc.egov.usda.gov/awdbRestApi/) is the authoritative source for the SCAN
(soil moisture), SNOTEL (snow) and related NRCS station networks. It replaces both the legacy
``wcc.nrcs.usda.gov`` endpoints and the third-party Powderlines API previously used for SNOTEL. No
authentication is required.
"""
import math
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests

AWDB_BASE_URL = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1"


def get_awdb_stations(network: str = "SCAN", state_code: Optional[str] = None,
                      active_only: bool = True) -> pd.DataFrame:
    """
    Lists stations of an AWDB network (e.g. SCAN or SNTL) with their coordinates and metadata.

    :param network: The AWDB network code, e.g. "SCAN" for soil moisture or "SNTL" for SNOTEL,
        defaults to "SCAN".
    :type network: str, optional
    :param state_code: Optional two-letter state code to filter by (e.g. "CO"), defaults to None.
    :type state_code: str, optional
    :param active_only: Whether to only return currently active stations, defaults to True.
    :type active_only: bool, optional
    :return: A dataframe with one row per station including stationTriplet, name, latitude, longitude,
        elevation and huc columns.
    :rtype: pd.DataFrame
    """
    triplet_filter = "*:" + (state_code if state_code else "*") + ":" + network
    params = {"stationTriplets": triplet_filter, "activeOnly": str(active_only).lower()}
    response = requests.get(AWDB_BASE_URL + "/stations", params=params, timeout=60)
    response.raise_for_status()
    return pd.DataFrame(response.json())


def find_nearest_awdb_station(latitude: float, longitude: float, network: str = "SCAN",
                              state_code: Optional[str] = None) -> Dict:
    """
    Finds the AWDB station of a network closest to a point (e.g. a USGS gauge or basin centroid).

    :param latitude: The latitude of the point in decimal degrees.
    :type latitude: float
    :param longitude: The longitude of the point in decimal degrees.
    :type longitude: float
    :param network: The AWDB network code to search, defaults to "SCAN".
    :type network: str, optional
    :param state_code: Optional two-letter state code to restrict the search, defaults to None.
    :type state_code: str, optional
    :return: The station record of the nearest station with an added "distance_km" key.
    :rtype: Dict
    """
    stations = get_awdb_stations(network=network, state_code=state_code)
    lat1, lon1 = math.radians(latitude), math.radians(longitude)
    lat2 = stations["latitude"].map(math.radians)
    lon2 = stations["longitude"].map(math.radians)
    hav = ((lat2 - lat1) / 2).map(math.sin) ** 2 + math.cos(lat1) * lat2.map(math.cos) * \
        ((lon2 - lon1) / 2).map(math.sin) ** 2
    distance_km = 2 * 6371.0 * hav.map(math.sqrt).map(math.asin)
    nearest = stations.iloc[distance_km.idxmin()].to_dict()
    nearest["distance_km"] = distance_km.min()
    return nearest


def get_awdb_element_data(station_triplet: str, elements: List[str], start_time: datetime,
                          end_time: datetime, duration: str = "HOURLY",
                          utc_offset_hours: Optional[float] = None) -> pd.DataFrame:
    """
    Fetches element time series from the AWDB data endpoint as a wide dataframe.

    Column names combine the element code and the sensor depth in inches when a depth is present, e.g.
    ``SMS_-2in`` for soil moisture two inches below the surface, otherwise just the element code (e.g.
    ``WTEQ``).

    :param station_triplet: The AWDB station triplet, e.g. "2017:CO:SCAN" or "713:CO:SNTL".
    :type station_triplet: str
    :param elements: The element codes to fetch, e.g. ["SMS:*"] for all soil moisture depths or
        ["WTEQ", "SNWD"] for SWE and snow depth.
    :type elements: List[str]
    :param start_time: The start of the requested period.
    :type start_time: datetime
    :param end_time: The end of the requested period.
    :type end_time: datetime
    :param duration: The AWDB duration name, e.g. "HOURLY" or "DAILY", defaults to "HOURLY".
    :type duration: str, optional
    :param utc_offset_hours: AWDB timestamps are in station-local standard time. If provided, this offset
        is subtracted to shift the "datetime" column to UTC (e.g. -8.0 for a station whose dataTimeZone
        is -8), defaults to None which leaves timestamps unshifted.
    :type utc_offset_hours: float, optional
    :return: A dataframe with a "datetime" column and one column per element/depth combination.
    :rtype: pd.DataFrame
    """
    params = {
        "stationTriplets": station_triplet,
        "elements": ",".join(elements),
        "duration": duration,
        "beginDate": start_time.strftime("%Y-%m-%d %H:%M"),
        "endDate": end_time.strftime("%Y-%m-%d %H:%M"),
    }
    response = requests.get(AWDB_BASE_URL + "/data", params=params, timeout=120)
    response.raise_for_status()
    payload = response.json()
    if not payload:
        return pd.DataFrame(columns=["datetime"])
    merged: Optional[pd.DataFrame] = None
    for element_block in payload[0].get("data", []):
        meta = element_block["stationElement"]
        name = meta["elementCode"]
        if meta.get("heightDepth") is not None:
            name = name + "_" + str(meta["heightDepth"]) + "in"
        series = pd.DataFrame(element_block["values"])
        if series.empty:
            continue
        series = series.rename(columns={"value": name})[["date", name]]
        merged = series if merged is None else merged.merge(series, on="date", how="outer")
    if merged is None:
        return pd.DataFrame(columns=["datetime"])
    merged["datetime"] = pd.to_datetime(merged["date"])
    if utc_offset_hours is not None:
        merged["datetime"] = merged["datetime"] - pd.Timedelta(hours=utc_offset_hours)
        merged["datetime"] = merged["datetime"].dt.tz_localize("UTC")
    return merged.drop(columns=["date"]).sort_values("datetime").reset_index(drop=True)


def get_scan_soil_moisture(station_triplet: str, start_time: datetime, end_time: datetime,
                           duration: str = "HOURLY",
                           utc_offset_hours: Optional[float] = None) -> pd.DataFrame:
    """
    Fetches soil moisture percent at all sensor depths for a SCAN (or SNOTEL) station.

    :param station_triplet: The AWDB station triplet, e.g. "2017:CO:SCAN".
    :type station_triplet: str
    :param start_time: The start of the requested period.
    :type start_time: datetime
    :param end_time: The end of the requested period.
    :type end_time: datetime
    :param duration: The AWDB duration name, defaults to "HOURLY".
    :type duration: str, optional
    :param utc_offset_hours: Optional station-local-to-UTC offset, see :func:`get_awdb_element_data`.
    :type utc_offset_hours: float, optional
    :return: A dataframe with a "datetime" column and one ``SMS_<depth>in`` column per sensor depth.
    :rtype: pd.DataFrame
    """
    return get_awdb_element_data(station_triplet, ["SMS:*"], start_time, end_time, duration,
                                 utc_offset_hours)


def get_snotel_awdb_data(station_triplet: str, start_time: datetime, end_time: datetime,
                         duration: str = "DAILY",
                         utc_offset_hours: Optional[float] = None) -> pd.DataFrame:
    """
    Fetches SWE, snow depth, precipitation and air temperature for a SNOTEL station via the AWDB API.

    This is the supported replacement for the third-party Powderlines API used by
    ``weather_scraping_functions.get_snotel_data``.

    :param station_triplet: The AWDB station triplet, e.g. "713:CO:SNTL".
    :type station_triplet: str
    :param start_time: The start of the requested period.
    :type start_time: datetime
    :param end_time: The end of the requested period.
    :type end_time: datetime
    :param duration: The AWDB duration name, defaults to "DAILY" (hourly is available for most sensors).
    :type duration: str, optional
    :param utc_offset_hours: Optional station-local-to-UTC offset, see :func:`get_awdb_element_data`.
    :type utc_offset_hours: float, optional
    :return: A dataframe with a "datetime" column and WTEQ (SWE, in), SNWD (snow depth, in),
        PREC (accumulated precip, in) and TOBS (air temperature, F) columns where available.
    :rtype: pd.DataFrame
    """
    return get_awdb_element_data(station_triplet, ["WTEQ", "SNWD", "PREC", "TOBS"], start_time,
                                 end_time, duration, utc_offset_hours)
