"""
Functions for scraping NLDAS-2 hourly forcing (radiation, temperature, precipitation, potential
evaporation, wind, humidity) as point time series via the NASA Giovanni Time Series API.

Giovanni (https://api.giovanni.earthdata.nasa.gov/timeseries) is the successor to the discontinued
Hydrology Data Rods service. It requires a free NASA Earthdata Login bearer token, read from the
``EARTHDATA_TOKEN`` environment variable by default (generate one at https://urs.earthdata.nasa.gov
under "Generate Token"). NLDAS-2 covers CONUS at 0.125 degrees hourly from 1979 to present.
"""
import io
import os
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests

GIOVANNI_TIMESERIES_URL = "https://api.giovanni.earthdata.nasa.gov/timeseries"

# Friendly name -> Giovanni data id for the NLDAS-2 primary forcing collection (v2.0).
NLDAS_FORCING_VARIABLES = {
    "shortwave_radiation": "NLDAS_FORA0125_H_2_0_SWdown",
    "longwave_radiation": "NLDAS_FORA0125_H_2_0_LWdown",
    "temperature": "NLDAS_FORA0125_H_2_0_Tair",
    "specific_humidity": "NLDAS_FORA0125_H_2_0_Qair",
    "precipitation": "NLDAS_FORA0125_H_2_0_Rainf",
    "potential_evaporation": "NLDAS_FORA0125_H_2_0_PotEvap",
    "wind_east": "NLDAS_FORA0125_H_2_0_Wind_E",
    "wind_north": "NLDAS_FORA0125_H_2_0_Wind_N",
}


def get_earthdata_token(token: Optional[str] = None) -> str:
    """
    Resolves the Earthdata bearer token from the argument or the EARTHDATA_TOKEN environment variable.

    :param token: An explicit token, defaults to None.
    :type token: str, optional
    :return: The bearer token string.
    :rtype: str
    """
    token = token or os.environ.get("EARTHDATA_TOKEN")
    if not token:
        raise RuntimeError(
            "No Earthdata token found. Set the EARTHDATA_TOKEN environment variable to a token "
            "generated at https://urs.earthdata.nasa.gov (Generate Token tab)."
        )
    return token


def parse_giovanni_csv(text: str, value_name: str) -> pd.DataFrame:
    """
    Parses the CSV payload returned by the Giovanni time series API into a dataframe.

    The payload contains metadata lines followed by a header line beginning with "Timestamp"; everything
    before that header is skipped. Fill values are converted to NaN.

    :param text: The raw CSV response text.
    :type text: str
    :param value_name: The column name to assign to the data values.
    :type value_name: str
    :return: A dataframe with "datetime" (UTC) and value_name columns.
    :rtype: pd.DataFrame
    """
    lines = text.splitlines()
    header_idx = next((i for i, line in enumerate(lines) if line.lower().startswith("timestamp")), None)
    if header_idx is None:
        raise ValueError("Could not find a Timestamp header in the Giovanni response. "
                         "Response began with: " + text[:200])
    df = pd.read_csv(io.StringIO("\n".join(lines[header_idx:])))
    df.columns = ["datetime", value_name]
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    df[value_name] = pd.to_numeric(df[value_name], errors="coerce")
    df.loc[df[value_name] <= -9990, value_name] = float("nan")
    return df


def get_giovanni_time_series(data_id: str, latitude: float, longitude: float, start_time: datetime,
                             end_time: datetime, token: Optional[str] = None,
                             value_name: str = "value") -> pd.DataFrame:
    """
    Fetches a single-variable point time series from the Giovanni time series API.

    :param data_id: The Giovanni data id, e.g. "NLDAS_FORA0125_H_2_0_SWdown".
    :type data_id: str
    :param latitude: The latitude of the point in decimal degrees.
    :type latitude: float
    :param longitude: The longitude of the point in decimal degrees.
    :type longitude: float
    :param start_time: The start of the requested period (UTC).
    :type start_time: datetime
    :param end_time: The end of the requested period (UTC).
    :type end_time: datetime
    :param token: An Earthdata bearer token, defaults to None which reads EARTHDATA_TOKEN.
    :type token: str, optional
    :param value_name: The column name for the values, defaults to "value".
    :type value_name: str, optional
    :return: A dataframe with "datetime" (UTC) and value_name columns.
    :rtype: pd.DataFrame
    """
    params = {
        "data": data_id,
        "location": "[" + str(latitude) + "," + str(longitude) + "]",
        "time": start_time.strftime("%Y-%m-%dT%H:%M:%S") + "/" + end_time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    headers = {"Authorization": "Bearer " + get_earthdata_token(token)}
    response = requests.get(GIOVANNI_TIMESERIES_URL, params=params, headers=headers, timeout=300)
    if response.status_code == 401:
        raise RuntimeError("Giovanni rejected the Earthdata token (401). Tokens expire after ~60 days; "
                           "regenerate one at https://urs.earthdata.nasa.gov.")
    response.raise_for_status()
    return parse_giovanni_csv(response.text, value_name)


def get_nldas_forcing(latitude: float, longitude: float, start_time: datetime, end_time: datetime,
                      variables: Optional[List[str]] = None, token: Optional[str] = None) -> pd.DataFrame:
    """
    Fetches multiple NLDAS-2 forcing variables for a point and merges them into one hourly dataframe.

    :param latitude: The latitude of the point in decimal degrees.
    :type latitude: float
    :param longitude: The longitude of the point in decimal degrees.
    :type longitude: float
    :param start_time: The start of the requested period (UTC).
    :type start_time: datetime
    :param end_time: The end of the requested period (UTC).
    :type end_time: datetime
    :param variables: Friendly variable names to fetch (keys of NLDAS_FORCING_VARIABLES), defaults to
        None which fetches all of them.
    :type variables: List[str], optional
    :param token: An Earthdata bearer token, defaults to None which reads EARTHDATA_TOKEN.
    :type token: str, optional
    :return: A dataframe with a "datetime" column (UTC) and one column per requested variable. Units
        follow NLDAS-2 conventions: W/m^2 for radiation, K for temperature, kg/m^2 (i.e. mm) per hour
        for precipitation and potential evaporation, m/s for wind and kg/kg for humidity.
    :rtype: pd.DataFrame
    """
    if variables is None:
        variables = list(NLDAS_FORCING_VARIABLES)
    unknown = [v for v in variables if v not in NLDAS_FORCING_VARIABLES]
    if unknown:
        raise KeyError("Unknown NLDAS variables " + str(unknown) + ". Valid options: " +
                       str(list(NLDAS_FORCING_VARIABLES)))
    merged: Optional[pd.DataFrame] = None
    for name in variables:
        series = get_giovanni_time_series(NLDAS_FORCING_VARIABLES[name], latitude, longitude,
                                          start_time, end_time, token=token, value_name=name)
        merged = series if merged is None else merged.merge(series, on="datetime", how="outer")
    return merged.sort_values("datetime").reset_index(drop=True)


def summarize_missing(df: pd.DataFrame) -> Dict[str, int]:
    """
    Counts missing values per column of a forcing dataframe (for data quality reporting).

    :param df: A dataframe as returned by :func:`get_nldas_forcing`.
    :type df: pd.DataFrame
    :return: A dict mapping column name to the number of NaN entries.
    :rtype: Dict[str, int]
    """
    return {col: int(df[col].isna().sum()) for col in df.columns if col != "datetime"}
