"""
Functions for scraping the University of Arizona daily 4-km CONUS SWE/snow-depth product (NSIDC-0719).

The product assimilates SNOTEL/COOP observations with PRISM temperature and precipitation. Two
distribution points are supported:

* The University of Arizona's own server (https://climate.arizona.edu/data/UA_SWE/), anonymous, with
  per-day ~100 KB netCDF files since WY1982 at ~2-day latency — the preferred path for basin time
  series because a whole-CONUS water-year file never needs to be downloaded. (The legacy NSIDC ECS
  hosts, including the n5eil01u/n5eil02u HTTPS and OPeNDAP endpoints, were decommissioned in 2025.)
* The NSIDC archive of finalized water years (https://daacdata.apps.nsidc.org/pub/DATASETS/
  nsidc0719_SWE_Snow_Depth_v1/), which requires an Earthdata Login. The bearer token is resolved with
  ``nldas_functions.get_earthdata_token`` (EARTHDATA_TOKEN env var, loaded from .env by
  ``build_pilot_dataset.load_dotenv``); note the account must additionally have the "NSIDC DAAC Data
  Access" application authorized in its Earthdata profile, otherwise URS refuses token logins.

Grids are 621 x 1405 cells at 1/24 degree with SWE in millimeters (int16, fill -999).
"""
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import requests

from nldas_functions import get_earthdata_token

UA_SWE_BASE_URL = "https://climate.arizona.edu/data/UA_SWE"
NSIDC_ARCHIVE_URL = "https://daacdata.apps.nsidc.org/pub/DATASETS/nsidc0719_SWE_Snow_Depth_v1"
SNOWVIEW_WATERSHED_URL = "https://snowview.arizona.edu/csv/Download/Watersheds/{huc8}.csv"
# Daily file maturity suffixes, most to least final; recent days may only exist as provisional/early.
DAILY_VARIANTS = ["stable", "provisional", "early"]
DEFAULT_UA_SWE_DIR = os.path.join("pilot_data", "ua_swe")
INCHES_TO_MM = 25.4


def water_year(date: datetime) -> int:
    """
    Returns the water year of a date (a water year starts October 1 of the previous calendar year).

    :param date: The date.
    :type date: datetime
    :return: The water year, e.g. 2024 for 2023-10-01 through 2024-09-30.
    :rtype: int
    """
    return date.year + 1 if date.month >= 10 else date.year


def ua_daily_file_url(date: datetime, variant: str = "stable") -> str:
    """
    Builds the URL of one day's 4-km UA SWE netCDF on the University of Arizona server.

    :param date: The date of the grid.
    :type date: datetime
    :param variant: The file maturity suffix ("stable", "provisional" or "early"), defaults
        to "stable".
    :type variant: str, optional
    :return: The file URL, e.g. ".../DailyData_4km/WY2024/UA_SWE_Depth_4km_v1_20240601_stable.nc".
    :rtype: str
    """
    return "/".join([UA_SWE_BASE_URL, "DailyData_4km", "WY" + str(water_year(date)),
                     "UA_SWE_Depth_4km_v1_" + date.strftime("%Y%m%d") + "_" + variant + ".nc"])


def download_ua_swe_day(date: datetime, out_dir: str = DEFAULT_UA_SWE_DIR,
                        variants: Optional[List[str]] = None) -> str:
    """
    Downloads one day's UA SWE netCDF, trying maturity variants in order and caching locally.

    :param date: The date of the grid.
    :type date: datetime
    :param out_dir: The directory files are cached in (gitignored pilot_data/ by default),
        defaults to "pilot_data/ua_swe".
    :type out_dir: str, optional
    :param variants: The maturity suffixes to try in order, defaults to None which tries
        stable, then provisional, then early.
    :type variants: List[str], optional
    :return: The local path of the netCDF file.
    :rtype: str
    """
    os.makedirs(out_dir, exist_ok=True)
    variants = DAILY_VARIANTS if variants is None else variants
    for variant in variants:
        url = ua_daily_file_url(date, variant=variant)
        local_path = os.path.join(out_dir, url.rsplit("/", 1)[-1])
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            return local_path
        response = requests.get(url, timeout=300)
        if response.status_code == 404:
            continue
        response.raise_for_status()
        with open(local_path, "wb") as ncfile:
            ncfile.write(response.content)
        return local_path
    raise FileNotFoundError("No UA SWE file for " + date.strftime("%Y-%m-%d") +
                            " under any variant " + str(variants))


def read_ua_swe_file(path: str, time_index: int = 0) -> Dict:
    """
    Reads SWE from a UA daily (or water-year) netCDF file into the shared grid-dict layout.

    :param path: The local netCDF path.
    :type path: str
    :param time_index: The time step to read (0 for daily files), defaults to 0.
    :type time_index: int, optional
    :return: A dict with "swe_mm" (2-D array, NaN where masked), "lats" (ascending), "lons" and
        "date".
    :rtype: Dict
    """
    import netCDF4
    with netCDF4.Dataset(path) as dataset:
        swe = np.ma.filled(dataset["SWE"][time_index].astype(np.float64), np.nan)
        lats = np.asarray(dataset["lat"][:], dtype=float)
        lons = np.asarray(dataset["lon"][:], dtype=float)
        days = float(np.ma.filled(dataset["time"][time_index], np.nan))
    date = datetime(1900, 1, 1) + timedelta(days=days)
    return {"swe_mm": swe, "lats": lats, "lons": lons, "date": date}


def get_ua_swe_grid(date: datetime, out_dir: str = DEFAULT_UA_SWE_DIR) -> Dict:
    """
    Downloads (if needed) and reads one day's UA 4-km SWE grid.

    :param date: The date of the grid.
    :type date: datetime
    :param out_dir: The directory files are cached in, defaults to "pilot_data/ua_swe".
    :type out_dir: str, optional
    :return: A grid dict as returned by :func:`read_ua_swe_file`.
    :rtype: Dict
    """
    return read_ua_swe_file(download_ua_swe_day(date, out_dir=out_dir))


def get_ua_basin_swe_series(geometry: Dict, start_date: datetime, end_date: datetime,
                            out_dir: str = DEFAULT_UA_SWE_DIR) -> pd.DataFrame:
    """
    Builds a daily basin-mean SWE series from per-day UA files (no whole-CONUS-year download).

    Days missing upstream (e.g. beyond the ~2-day latency) are skipped rather than raised.

    :param geometry: The basin GeoJSON polygon from ``usgs_scraping_functions.get_basin_boundary``.
    :type geometry: Dict
    :param start_date: The first date of the series.
    :type start_date: datetime
    :param end_date: The last date of the series.
    :type end_date: datetime
    :param out_dir: The directory files are cached in, defaults to "pilot_data/ua_swe".
    :type out_dir: str, optional
    :return: A dataframe with "datetime" and "ua_swe_mm" columns.
    :rtype: pd.DataFrame
    """
    from snodas_functions import basin_mean_swe
    records = []
    date = start_date
    while date <= end_date:
        try:
            grid = get_ua_swe_grid(date, out_dir=out_dir)
        except FileNotFoundError:
            date += timedelta(days=1)
            continue
        records.append({"datetime": grid["date"], "ua_swe_mm": basin_mean_swe(grid, geometry=geometry)})
        date += timedelta(days=1)
    return pd.DataFrame(records, columns=["datetime", "ua_swe_mm"])


def download_ua_water_year(year: int, out_dir: str = DEFAULT_UA_SWE_DIR, source: str = "ua",
                           token: Optional[str] = None) -> str:
    """
    Downloads a whole water year of 4-km SWE as one netCDF (hundreds of MB).

    The "ua" source (default) is the University of Arizona server and needs no authentication. The
    "nsidc" source is the NSIDC archive of finalized water years and authenticates with the
    Earthdata bearer token; URS only honors tokens for accounts that have authorized the NSIDC DAAC
    application (log in once at https://daacdata.apps.nsidc.org in a browser to authorize it).

    :param year: The water year, e.g. 2024.
    :type year: int
    :param out_dir: The directory files are cached in, defaults to "pilot_data/ua_swe".
    :type out_dir: str, optional
    :param source: "ua" or "nsidc", defaults to "ua".
    :type source: str, optional
    :param token: An Earthdata bearer token for the "nsidc" source, defaults to None which reads
        EARTHDATA_TOKEN.
    :type token: str, optional
    :return: The local path of the netCDF file.
    :rtype: str
    """
    if source == "ua":
        url = UA_SWE_BASE_URL + "/WYData_4km/UA_SWE_Depth_WY" + str(year) + ".nc"
        headers = {}
    elif source == "nsidc":
        url = NSIDC_ARCHIVE_URL + "/4km_SWE_Depth_WY" + str(year) + "_v01.nc"
        headers = {"Authorization": "Bearer " + get_earthdata_token(token)}
    else:
        raise ValueError("source must be 'ua' or 'nsidc', got " + repr(source))
    os.makedirs(out_dir, exist_ok=True)
    local_path = os.path.join(out_dir, url.rsplit("/", 1)[-1])
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        return local_path
    session = requests.Session()
    # requests drops Authorization on cross-host redirects; re-add it so the token survives the
    # daacdata -> urs.earthdata.nasa.gov OAuth hop.
    response = session.get(url, headers=headers, timeout=3600, allow_redirects=False)
    hops = 0
    while response.is_redirect and hops < 10:
        response = session.get(response.headers["location"], headers=headers, timeout=3600,
                               allow_redirects=False)
        hops += 1
    if response.status_code == 401:
        raise RuntimeError("Earthdata refused the token for the NSIDC archive (401). The account "
                           "must have the NSIDC DAAC application authorized: log in once at " +
                           NSIDC_ARCHIVE_URL + " in a browser, or use the anonymous 'ua' source.")
    response.raise_for_status()
    with open(local_path, "wb") as ncfile:
        ncfile.write(response.content)
    return local_path


def get_snowview_watershed_series(huc8: str) -> pd.DataFrame:
    """
    Fetches the UA SnowView daily watershed-mean SWE series for a HUC8 watershed (anonymous).

    This is the University of Arizona's own basin aggregation of the same 4-km product and is a
    convenient cross-check for the gridded basin means (note a HUC8 is usually larger than a gauge's
    upstream basin).

    :param huc8: The 8-digit hydrologic unit code, e.g. "10190007" for the Cache la Poudre.
    :type huc8: str
    :return: A dataframe with "datetime", "swe_mm" and "wy_precip_mm" columns.
    :rtype: pd.DataFrame
    """
    response = requests.get(SNOWVIEW_WATERSHED_URL.format(huc8=huc8), timeout=300)
    response.raise_for_status()
    import io
    frame = pd.read_csv(io.StringIO(response.text))
    frame.columns = ["datetime", "wy_precip_mm", "swe_mm"]
    frame["datetime"] = pd.to_datetime(frame["datetime"], format="%m/%d/%Y")
    frame["swe_mm"] = pd.to_numeric(frame["swe_mm"], errors="coerce") * INCHES_TO_MM
    frame["wy_precip_mm"] = pd.to_numeric(frame["wy_precip_mm"], errors="coerce") * INCHES_TO_MM
    return frame[["datetime", "swe_mm", "wy_precip_mm"]]
