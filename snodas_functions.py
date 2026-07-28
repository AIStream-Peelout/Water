"""
Functions for scraping NOAA/NOHRSC SNODAS daily 1-km CONUS snow grids (NSIDC dataset G02158).

SNODAS is served anonymously from https://noaadata.apps.nsidc.org/NOAA/G02158/ as one .tar per day
(masked/ for the CONUS-clipped grids, unmasked/ for the full modeling domain). Each tar holds a pair
of files per product: a gzipped flat binary grid (16-bit signed big-endian integers) and a gzipped
NOHRSC text header carrying the grid geometry. Snow water equivalent is product code 1034 in
millimeters; masked grids are 3351 rows x 6935 columns at 1/120 degree (~1 km) starting 2003-09-30.
"""
import gzip
import os
import re
import tarfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import requests

SNODAS_BASE_URL = "https://noaadata.apps.nsidc.org/NOAA/G02158"
SWE_PRODUCT_CODE = "1034"
SNODAS_FILL_VALUE = -9999
MONTH_DIRS = ["01_Jan", "02_Feb", "03_Mar", "04_Apr", "05_May", "06_Jun", "07_Jul", "08_Aug",
              "09_Sep", "10_Oct", "11_Nov", "12_Dec"]
DEFAULT_SNODAS_DIR = os.path.join("pilot_data", "snodas")


def snodas_tar_url(date: datetime, masked: bool = True) -> str:
    """
    Builds the download URL of a day's SNODAS tar archive.

    :param date: The date of the grids.
    :type date: datetime
    :param masked: Whether to use the CONUS-clipped "masked" archive (True) or the full-domain
        "unmasked" one, defaults to True.
    :type masked: bool, optional
    :return: The archive URL, e.g. ".../masked/2024/06_Jun/SNODAS_20240601.tar".
    :rtype: str
    """
    domain = "masked" if masked else "unmasked"
    name = ("SNODAS_" if masked else "SNODAS_unmasked_") + date.strftime("%Y%m%d") + ".tar"
    return "/".join([SNODAS_BASE_URL, domain, date.strftime("%Y"), MONTH_DIRS[date.month - 1], name])


def download_snodas_tar(date: datetime, out_dir: str = DEFAULT_SNODAS_DIR,
                        masked: bool = True) -> str:
    """
    Downloads a day's SNODAS tar archive, skipping the download when the file already exists.

    :param date: The date of the grids.
    :type date: datetime
    :param out_dir: The directory to save the archive in (gitignored pilot_data/ by default),
        defaults to "pilot_data/snodas".
    :type out_dir: str, optional
    :param masked: Whether to fetch the masked (CONUS) archive, defaults to True.
    :type masked: bool, optional
    :return: The local path of the tar archive.
    :rtype: str
    """
    url = snodas_tar_url(date, masked=masked)
    os.makedirs(out_dir, exist_ok=True)
    local_path = os.path.join(out_dir, url.rsplit("/", 1)[-1])
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        return local_path
    response = requests.get(url, timeout=600)
    if response.status_code == 404:
        raise FileNotFoundError("No SNODAS archive for " + date.strftime("%Y-%m-%d") + " at " + url)
    response.raise_for_status()
    with open(local_path, "wb") as archive:
        archive.write(response.content)
    return local_path


def parse_snodas_header(text: str) -> Dict:
    """
    Parses a NOHRSC text header ("key: value" lines) into a dict with numeric coercion.

    :param text: The header file contents.
    :type text: str
    :return: A dict mapping header keys to str/int/float values.
    :rtype: Dict
    """
    header: Dict = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        value = value.strip()
        if re.fullmatch(r"-?\d+", value):
            header[key.strip()] = int(value)
        elif re.fullmatch(r"-?\d*\.\d+(e[+-]?\d+)?", value, flags=re.IGNORECASE):
            header[key.strip()] = float(value)
        else:
            header[key.strip()] = value
    return header


def read_snodas_swe(tar_path: str) -> Dict:
    """
    Extracts and parses the SWE grid (product 1034) from a SNODAS tar archive.

    The flat binary grid is 16-bit signed big-endian; fill values become NaN and the header's slope/
    intercept (identity for SWE, which is already in millimeters) are applied. Cell-center
    coordinates are derived from the header benchmark position and resolution.

    :param tar_path: The local path of a daily SNODAS tar archive.
    :type tar_path: str
    :return: A dict with "swe_mm" (2-D array, north to south), "lats" (descending), "lons",
        "date" and the parsed "header".
    :rtype: Dict
    """
    with tarfile.open(tar_path) as archive:
        names = archive.getnames()
        data_name = _member_for_product(names, ".dat")
        header_name = _member_for_product(names, ".txt")
        data_bytes = archive.extractfile(data_name).read()
        header_bytes = archive.extractfile(header_name).read()
    if not data_name.endswith(".gz"):
        data_bytes = gzip.compress(data_bytes)
    if not header_name.endswith(".gz"):
        header_bytes = gzip.compress(header_bytes)
    return swe_grid_from_members(header_bytes, data_bytes)


def swe_grid_from_members(header_gz: bytes, data_gz: bytes) -> Dict:
    """
    Parses a SNODAS SWE grid from the raw gzipped header/data member pair of a daily tar.

    :param header_gz: The gzipped NOHRSC text header bytes.
    :type header_gz: bytes
    :param data_gz: The gzipped flat-binary grid bytes (16-bit signed big-endian).
    :type data_gz: bytes
    :return: A grid dict with "swe_mm" (2-D array, north to south), "lats" (descending), "lons",
        "date" and the parsed "header".
    :rtype: Dict
    """
    header_bytes = gzip.decompress(header_gz)
    data_bytes = gzip.decompress(data_gz)
    header = parse_snodas_header(header_bytes.decode("ascii", errors="replace"))
    rows, cols = header["Number of rows"], header["Number of columns"]
    raw = np.frombuffer(data_bytes, dtype=">i2").astype(np.float64).reshape(rows, cols)
    raw[raw == SNODAS_FILL_VALUE] = np.nan
    swe = raw * float(header.get("Data slope", 1.0)) + float(header.get("Data intercept", 0.0))
    x_res, y_res = header["X-axis resolution"], header["Y-axis resolution"]
    lons = header["Benchmark x-axis coordinate"] + np.arange(cols) * x_res
    lats = header["Benchmark y-axis coordinate"] - np.arange(rows) * y_res
    date = datetime(header["Start year"], header["Start month"], header["Start day"])
    return {"swe_mm": swe, "lats": lats, "lons": lons, "date": date, "header": header}


def _member_for_product(names: List[str], extension: str, product_code: str = SWE_PRODUCT_CODE) -> str:
    """
    Finds the tar member of a SNODAS product with a given extension (.dat or .txt, possibly .gz).

    :param names: The member names of the archive.
    :type names: List[str]
    :param extension: The bare extension to match, e.g. ".dat".
    :type extension: str
    :param product_code: The SNODAS product code, defaults to "1034" (SWE).
    :type product_code: str, optional
    :return: The matching member name.
    :rtype: str
    """
    for name in names:
        if "ssmv1" + product_code in name and (name.endswith(extension) or
                                               name.endswith(extension + ".gz")):
            return name
    raise KeyError("No member for product " + product_code + extension + " among " + str(names[:6]))


def get_snodas_swe_grid(date: datetime, out_dir: str = DEFAULT_SNODAS_DIR,
                        masked: bool = True) -> Dict:
    """
    Downloads (if needed) and parses one day's SNODAS SWE grid.

    :param date: The date of the grid.
    :type date: datetime
    :param out_dir: The directory tar archives are cached in, defaults to "pilot_data/snodas".
    :type out_dir: str, optional
    :param masked: Whether to use the masked (CONUS) archive, defaults to True.
    :type masked: bool, optional
    :return: A grid dict as returned by :func:`read_snodas_swe`.
    :rtype: Dict
    """
    return read_snodas_swe(download_snodas_tar(date, out_dir=out_dir, masked=masked))


def basin_mean_swe(grid: Dict, geometry: Optional[Dict] = None,
                   bbox: Optional[List[float]] = None) -> float:
    """
    Averages a SNODAS-style SWE grid over a basin polygon or bounding box.

    :param grid: A grid dict with "swe_mm", "lats" and "lons" (e.g. from
        :func:`get_snodas_swe_grid` or ``ua_swe_functions.read_ua_swe_file``).
    :type grid: Dict
    :param geometry: The basin GeoJSON polygon from ``usgs_scraping_functions.get_basin_boundary``
        (takes precedence over bbox), defaults to None.
    :type geometry: Dict, optional
    :param bbox: A (min_lon, min_lat, max_lon, max_lat) bounding box, defaults to None.
    :type bbox: List[float], optional
    :return: The mean SWE in millimeters over valid (non-NaN) cells inside the basin.
    :rtype: float
    """
    from dem_functions import polygon_mask
    lats, lons = np.asarray(grid["lats"]), np.asarray(grid["lons"])
    if geometry is not None:
        mask = polygon_mask(lats, lons, geometry)
    elif bbox is not None:
        min_lon, min_lat, max_lon, max_lat = bbox
        mask = ((lats >= min_lat) & (lats <= max_lat))[:, None] & \
               ((lons >= min_lon) & (lons <= max_lon))[None, :]
    else:
        raise ValueError("Provide either geometry or bbox")
    values = np.asarray(grid["swe_mm"], dtype=float)[mask]
    values = values[~np.isnan(values)]
    if len(values) == 0:
        raise ValueError("No valid SWE cells inside the basin")
    return float(values.mean())


def get_snodas_basin_series(geometry: Dict, start_date: datetime, end_date: datetime,
                            out_dir: str = DEFAULT_SNODAS_DIR, masked: bool = True) -> pd.DataFrame:
    """
    Builds a daily basin-mean SWE time series from SNODAS between two dates (inclusive).

    Days whose archive is missing upstream are skipped rather than raised.

    :param geometry: The basin GeoJSON polygon.
    :type geometry: Dict
    :param start_date: The first date of the series.
    :type start_date: datetime
    :param end_date: The last date of the series.
    :type end_date: datetime
    :param out_dir: The directory tar archives are cached in, defaults to "pilot_data/snodas".
    :type out_dir: str, optional
    :param masked: Whether to use the masked (CONUS) archives, defaults to True.
    :type masked: bool, optional
    :return: A dataframe with "datetime" and "snodas_swe_mm" columns.
    :rtype: pd.DataFrame
    """
    records = []
    date = start_date
    while date <= end_date:
        try:
            grid = get_snodas_swe_grid(date, out_dir=out_dir, masked=masked)
        except FileNotFoundError:
            date += timedelta(days=1)
            continue
        records.append({"datetime": grid["date"],
                        "snodas_swe_mm": basin_mean_swe(grid, geometry=geometry)})
        date += timedelta(days=1)
    return pd.DataFrame(records, columns=["datetime", "snodas_swe_mm"])
