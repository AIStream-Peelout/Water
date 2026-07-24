"""
Functions for joining GAGES-II static basin attributes to USGS gauges.

GAGES-II ("Geospatial Attributes of Gages for Evaluating Streamflow, version II", Falcone 2011) provides
several hundred pre-computed basin characteristics — topography, soils, climate normals, geology,
hydrology — for 9,067 conterminous-US gauges. It complements the live NWIS site service (drainage area,
altitude) with the slope/soils attributes the catchment embedding needs, without any GIS processing.

The archive is a single ~55 MB zip hosted by USGS; download it once with :func:`download_gages2` and
reuse the cached copy. Attribute tables live in a nested ``spreadsheets-in-csv-format.zip`` and are
latin-1 encoded with the gauge id in a ``STAID`` column.
"""
import io
import os
import zipfile
from typing import Dict, List, Optional

import pandas as pd
import requests

GAGES2_URL = "https://water.usgs.gov/GIS/dsdl/basinchar_and_report_sept_2011.zip"
INNER_ZIP_NAME = "spreadsheets-in-csv-format.zip"

# Default attribute tables joined for a gauge; a useful static feature set for catchment embeddings.
DEFAULT_TABLES = ["conterm_basinid.txt", "conterm_topo.txt", "conterm_soils.txt",
                  "conterm_climate.txt", "conterm_geology.txt", "conterm_hydro.txt"]


def download_gages2(zip_path: str) -> str:
    """
    Downloads the GAGES-II basin characteristics archive if it is not already cached.

    :param zip_path: The local path to store (or find) the archive at.
    :type zip_path: str
    :return: The zip_path, for chaining.
    :rtype: str
    """
    if os.path.exists(zip_path):
        return zip_path
    directory = os.path.dirname(zip_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    print("Downloading GAGES-II archive (~55 MB) from " + GAGES2_URL)
    response = requests.get(GAGES2_URL, timeout=600)
    response.raise_for_status()
    with open(zip_path, "wb") as f:
        f.write(response.content)
    return zip_path


def load_gages2_table(table_name: str, zip_path: str) -> pd.DataFrame:
    """
    Loads one attribute table from the (nested) GAGES-II archive.

    :param table_name: The table file name, e.g. "conterm_topo.txt".
    :type table_name: str
    :param zip_path: Path to the downloaded GAGES-II archive.
    :type zip_path: str
    :return: The table as a dataframe with STAID parsed as a zero-padded string.
    :rtype: pd.DataFrame
    """
    with zipfile.ZipFile(zip_path) as outer:
        with zipfile.ZipFile(io.BytesIO(outer.read(INNER_ZIP_NAME))) as inner:
            return pd.read_csv(io.BytesIO(inner.read(table_name)), dtype={"STAID": str},
                               encoding="latin-1")


def get_gages2_attributes(site_number: str, zip_path: str,
                          tables: Optional[List[str]] = None) -> Dict:
    """
    Returns the merged GAGES-II attributes of one gauge across the requested tables.

    :param site_number: The USGS gauge site number, e.g. "06752260".
    :type site_number: str
    :param zip_path: Path to the downloaded GAGES-II archive (see :func:`download_gages2`).
    :type zip_path: str
    :param tables: The attribute tables to join, defaults to None which uses DEFAULT_TABLES.
    :type tables: List[str], optional
    :return: A dict of attribute name to value for the gauge.
    :rtype: Dict
    """
    if tables is None:
        tables = DEFAULT_TABLES
    attributes: Dict = {}
    for table_name in tables:
        table = load_gages2_table(table_name, zip_path)
        row = table[table["STAID"] == site_number]
        if row.empty:
            raise KeyError("Gauge " + site_number + " not found in GAGES-II table " + table_name +
                           " (GAGES-II covers 9,067 conterminous-US gauges; this gauge is not one of them)")
        record = row.iloc[0].to_dict()
        record.pop("STAID", None)
        attributes.update(record)
    return attributes
