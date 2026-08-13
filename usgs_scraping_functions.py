
import pandas as pd
from datetime import datetime
from typing import Tuple, Dict
import requests
import boto3
from botocore import UNSIGNED
from botocore.config import Config


def make_usgs_data(start_date: datetime, end_date: datetime, site_number: str):
    """
    Function that scrapes data from gages from a specified start_time THROUGH
    a specified end_time. Returns hourly df of river flow data. For instance:

    ..
    from datetime import datetime
    df = make_usgs_data(datetime(2020, 5, 1), datetime(2021, 5, 1) "01010500")
    df[1:] # would return time stamps of 5/1 in fifteen minute increments (e.g 97)
    len(df[1:]) # 96 The first row is a junk row and real data starts second row (e.g. 96)
    ..

    """
    # //waterservices.usgs.gov/nwis/iv/?format=rdb,1.0&sites={}&startDT={}&endDT={}&parameterCd=00060,00065,00045&siteStatus=all
    base_url = "http://waterservices.usgs.gov/nwis/iv/?format=rdb,1.0&sites={}&startDT={}&endDT={}&parameterCd=00060,00065,00045&siteStatus=all"
    full_url = base_url.format(site_number, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    print("Getting request from USGS")
    print(full_url)
    # Bounded read timeout: a silently stalled NWIS connection would otherwise hang the whole
    # fleet scrape indefinitely (no bytes ever arrive, no error). 180s tolerates large multi-year
    # chunks while turning a dead connection into a retryable per-chunk timeout.
    r = requests.get(full_url, timeout=180)
    with open(site_number + ".txt", "w") as f:
        f.write(r.text)
    response_data = process_response_text(site_number + ".txt")
    return create_csv(response_data[0], response_data[1], site_number)


def column_renamer(x):
    """_summary_

    :param x: The column names of the dataframe as a string
    :type x: _str
    :return: _description_
    :rtype: _type_
    """
    code_converter_1 = {"00060": "cfs", "00065": "height", "00045": "precip_usgs"}
    split_x = x.split("_")
    if len(split_x) > 1:
        if split_x[1] in code_converter_1 and "cd" not in x:
            return code_converter_1[split_x[1]]
    return x


def rename_cols(df) -> pd.DataFrame:
    """_summary_

    :param df: _description_
    :type df: _type_
    :return: _description_
    :rtype: pd.DataFrame
    """
    df.columns = df.columns.map(column_renamer)
    return df


def process_response_text(file_name: str)->Tuple[str, Dict]:
    """_summary_

    :param file_name: _description_
    :type file_name: str
    :return: _description_
    :rtype: Tuple[str, Dict]
    """
    extractive_params = {}
    with open(file_name, "r") as f:
        lines = f.readlines()
        i = 0
        params = False
        # A window with no data returns only comment lines, so the loop must also stop at EOF.
        while i < len(lines) and "#" in lines[i]:
            # TODO figure out getting height and discharge code efficently
            the_split_line = lines[i].split()[1:]
            if params:
                print(the_split_line)
                if len(the_split_line)<2:
                    params = False
                else:
                    extractive_params[the_split_line[0]+"_"+the_split_line[1]] = df_label(the_split_line[2])
            if len(the_split_line)>2:
                if the_split_line[0] == "TS":
                    params = True
            i += 1
        with open(file_name.split(".")[0] + "data.tsv", "w") as t:
            t.write("".join(lines[i:]))
        return file_name.split(".")[0] + "data.tsv", extractive_params


def df_label(usgs_text: str) -> str:
    """_summary_

    :param usgs_text: _description
    :type usgs_text: str
    :return: _description_
    :rtype: str
    """
    usgs_text = usgs_text.replace(",", "")
    if usgs_text == "Discharge":
        return "cfs"
    elif usgs_text == "Gage":
        return "height"
    else:
        return usgs_text


def create_csv(file_path: str, params_names: dict, site_number: str):
    """
    Function that creates the final version of the CSV file
    Assigns
    """
    print(params_names)
    import os
    if os.path.getsize(file_path) == 0:
        pd.DataFrame().to_csv(site_number + "_flow_data.csv")
        return pd.DataFrame()
    df = pd.read_csv(file_path, sep="\t")
    for key, value in params_names.items():
        df[value] = df[key]
    df.to_csv(site_number + "_flow_data.csv")
    return df


def get_site_metadata(site_number: str) -> Dict:
    """
    Fetches static gauge attributes from the NWIS site service (expanded output).

    Useful static catchment attributes include drain_area_va (drainage area, sq mi),
    contrib_drain_area_va, alt_va (gauge altitude, ft), huc_cd, dec_lat_va and dec_long_va.

    :param site_number: The USGS gauge site number, e.g. "01010500".
    :type site_number: str
    :return: A dict of the site's attributes with numeric fields parsed to floats where possible.
    :rtype: Dict
    """
    base_url = "https://waterservices.usgs.gov/nwis/site/?format=rdb&sites={}&siteOutput=expanded"
    response = requests.get(base_url.format(site_number), timeout=60)
    response.raise_for_status()
    lines = [line for line in response.text.splitlines() if not line.startswith("#")]
    if len(lines) < 3:
        raise ValueError("NWIS returned no site data for " + site_number)
    header = lines[0].split("\t")
    values = lines[2].split("\t")
    metadata: Dict = {}
    numeric_fields = {"dec_lat_va", "dec_long_va", "alt_va", "drain_area_va", "contrib_drain_area_va"}
    for key, value in zip(header, values):
        if key in numeric_fields:
            try:
                metadata[key] = float(value)
            except ValueError:
                metadata[key] = None
        else:
            metadata[key] = value
    return metadata


def get_period_of_record(site_number: str) -> Dict[str, Dict[str, str]]:
    """
    Fetches the period of record per parameter and data type from the NWIS series catalog.

    The instantaneous ("uv") record usually starts decades after the daily ("dv") record — e.g. gauge
    06752260 has daily flow from 1975 but 15-minute flow only from 1987 — so long hourly scrapes should
    start at the uv begin date.

    :param site_number: The USGS gauge site number, e.g. "06752260".
    :type site_number: str
    :return: A dict keyed by "<data_type>_<param>" (e.g. "uv_00060") with "begin_date", "end_date" and
        "count" for each series.
    :rtype: Dict[str, Dict[str, str]]
    """
    base_url = ("https://waterservices.usgs.gov/nwis/site/?format=rdb&sites={}"
                "&seriesCatalogOutput=true&siteStatus=all")
    response = requests.get(base_url.format(site_number), timeout=60)
    response.raise_for_status()
    lines = [line for line in response.text.splitlines() if not line.startswith("#")]
    header = lines[0].split("\t")
    idx = {name: header.index(name) for name in
           ("data_type_cd", "parm_cd", "begin_date", "end_date", "count_nu")}
    catalog: Dict[str, Dict[str, str]] = {}
    for line in lines[2:]:
        fields = line.split("\t")
        if len(fields) < len(header) or not fields[idx["parm_cd"]]:
            continue
        key = fields[idx["data_type_cd"]] + "_" + fields[idx["parm_cd"]]
        entry = {"begin_date": fields[idx["begin_date"]], "end_date": fields[idx["end_date"]],
                 "count": fields[idx["count_nu"]]}
        # A site can have multiple entries per series (e.g. multiple sensors); keep the earliest begin.
        if key not in catalog or entry["begin_date"] < catalog[key]["begin_date"]:
            catalog[key] = entry
    return catalog


def get_basin_boundary(site_number: str) -> Dict:
    """
    Fetches the upstream basin boundary polygon for a gauge from the USGS NLDI service.

    The polygon defines the catchment footprint and is the natural bounding geometry for extracting
    satellite image patches around a gauge.

    :param site_number: The USGS gauge site number, e.g. "01010500".
    :type site_number: str
    :return: The GeoJSON geometry dict (type + coordinates) of the basin polygon.
    :rtype: Dict
    """
    base_url = "https://api.water.usgs.gov/nldi/linked-data/nwissite/USGS-{}/basin?f=json"
    response = requests.get(base_url.format(site_number), timeout=120)
    response.raise_for_status()
    features = response.json().get("features", [])
    if not features:
        raise ValueError("NLDI returned no basin for site " + site_number)
    return features[0]["geometry"]


def basin_bounding_box(geometry: Dict, buffer_degrees: float = 0.0) -> Tuple[float, float, float, float]:
    """
    Computes the (min_lon, min_lat, max_lon, max_lat) bounding box of a GeoJSON polygon geometry.

    :param geometry: A GeoJSON Polygon or MultiPolygon geometry dict.
    :type geometry: Dict
    :param buffer_degrees: A buffer to expand the box on every side in decimal degrees, defaults to 0.0.
    :type buffer_degrees: float, optional
    :return: The bounding box as (min_lon, min_lat, max_lon, max_lat).
    :rtype: Tuple[float, float, float, float]
    """
    rings = geometry["coordinates"]
    if geometry.get("type") == "Polygon":
        rings = [rings]
    points = [point for polygon in rings for ring in polygon for point in ring]
    lons = [point[0] for point in points]
    lats = [point[1] for point in points]
    return (min(lons) - buffer_degrees, min(lats) - buffer_degrees,
            max(lons) + buffer_degrees, max(lats) + buffer_degrees)
