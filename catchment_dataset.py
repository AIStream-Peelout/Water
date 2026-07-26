"""
Builds per-gauge "catchment bundles": the training-ready unit of data for the catchment foundation model.

A bundle consists of three artifacts written to an output directory:

1. ``<site>_static.json`` — static attributes (drainage area, altitude, HUC, coordinates, nearest SCAN
   station) from the NWIS site service and the AWDB station inventory.
2. ``<site>_basin.geojson`` — the upstream basin polygon from the USGS NLDI service (the footprint for
   satellite patch extraction).
3. ``<site>_hourly.csv`` — an hourly UTC dataframe joining USGS streamflow with SCAN soil moisture and,
   when an Earthdata token is available, NLDAS-2 forcing (radiation, temperature, precipitation and
   potential evaporation).

This module deliberately avoids the BigQuery/Redis dependencies of ``scraping_functions`` so bundles can
be built in any environment; ASOS weather joining remains the job of ``HydroScraper``.
"""
import json
import os
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
import pytz

import requests

from awdb_functions import find_best_scan_station, get_element_begin_date, get_scan_soil_moisture
from gages2_functions import download_gages2, gauge_in_gages2, get_gages2_attributes
from nldas_functions import get_nldas_forcing
from scrape_text import timezone_map
from weather_scraping_functions import FIPS_TO_STATE, find_nearest_asos_station, get_hourly_asos
from usgs_scraping_functions import (basin_bounding_box, get_basin_boundary, get_period_of_record,
                                     get_site_metadata, make_usgs_data, rename_cols)

# NLDAS-2 primary forcing coverage begins here (hourly, CONUS-wide).
NLDAS_BEGIN_DATE = "1979-01-02"



def usgs_to_hourly_utc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts a raw USGS instantaneous-values dataframe to an hourly UTC dataframe.

    Mirrors ``HydroScraper.process_intermediate_csv`` (junk-row removal, timezone conversion, numeric
    parsing, hourly filtering) without requiring the BigQuery/Redis imports of that module.

    :param df: The dataframe returned by :func:`usgs_scraping_functions.make_usgs_data` after
        :func:`usgs_scraping_functions.rename_cols`.
    :type df: pd.DataFrame
    :return: An hourly dataframe with a tz-aware UTC "datetime" column and numeric flow columns.
    :rtype: pd.DataFrame
    """
    if len(df) < 2 or "tz_cd" not in df.columns or "datetime" not in df.columns:
        return pd.DataFrame(columns=["datetime", "cfs"])
    df = df.iloc[1:].copy()
    local_zone = pytz.timezone(timezone_map[df["tz_cd"].iloc[0]])
    df["datetime"] = df["datetime"].map(
        lambda x: local_zone.localize(datetime.strptime(x, "%Y-%m-%d %H:%M")).astimezone(pytz.UTC))
    for column in ("cfs", "height", "precip_usgs"):
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    hourly = df[df["datetime"].map(lambda x: x.minute) == 0].reset_index(drop=True)
    keep = ["datetime"] + [c for c in ("cfs", "height", "precip_usgs") if c in hourly.columns]
    return hourly[keep]


def discover_catchment(site_number: str, include_basin: bool = True, include_asos: bool = True,
                       include_soil_moisture: bool = True,
                       gages2_zip_path: Optional[str] = None) -> Dict:
    """
    Performs the one-time, time-independent discovery for a gauge: static attributes, basin polygon
    and nearest ASOS/SCAN stations.

    Long scrapes call this once and then :func:`fetch_hourly_chunk` per time window, so station
    discovery and metadata services are not re-queried for every chunk.

    :param site_number: The USGS gauge site number.
    :type site_number: str
    :param include_basin: Whether to fetch the NLDI basin polygon, defaults to True.
    :type include_basin: bool, optional
    :param include_asos: Whether to locate the nearest ASOS station, defaults to True.
    :type include_asos: bool, optional
    :param include_soil_moisture: Whether to locate the nearest SCAN station, defaults to True.
    :type include_soil_moisture: bool, optional
    :param gages2_zip_path: Optional GAGES-II archive path for the static attribute join,
        defaults to None.
    :type gages2_zip_path: str, optional
    :return: A dict with "static", "basin_geometry", "asos_station" and "scan_station" keys (the
        latter three may be None when disabled or unavailable).
    :rtype: Dict
    """
    static = get_site_metadata(site_number)
    latitude, longitude = static["dec_lat_va"], static["dec_long_va"]

    if gages2_zip_path is not None:
        download_gages2(gages2_zip_path)
        static["gages2_available"] = gauge_in_gages2(site_number, gages2_zip_path)
        if static["gages2_available"]:
            for key, value in get_gages2_attributes(site_number, gages2_zip_path).items():
                static["gages2_" + key] = value

    basin_geometry = None
    if include_basin:
        # Not every gauge is indexed by NLDI; a missing basin must not fail a fleet scrape.
        try:
            basin_geometry = get_basin_boundary(site_number)
        except (requests.HTTPError, ValueError):
            static["basin_available"] = False
        if basin_geometry is not None:
            static["basin_bbox"] = basin_bounding_box(basin_geometry, buffer_degrees=0.05)

    asos_station = None
    if include_asos:
        asos_station = find_nearest_asos_station(latitude, longitude, FIPS_TO_STATE[static["state_cd"]])
        static["asos_station_id"] = asos_station["station_id"]
        static["asos_distance_km"] = asos_station["distance_km"]

    scan_station = None
    if include_soil_moisture:
        # Prefer the station with the longest soil moisture record within range over the pure nearest.
        scan_station = find_best_scan_station(latitude, longitude)
        if scan_station is not None:
            static["scan_triplet"] = scan_station["stationTriplet"]
            static["scan_distance_km"] = scan_station["distance_km"]
            static["scan_sms_begin"] = scan_station["element_begin"]

    return {"static": static, "basin_geometry": basin_geometry, "asos_station": asos_station,
            "scan_station": scan_station}


def get_data_availability(site_number: str, discovery: Optional[Dict] = None,
                          include_sentinel: bool = True, include_camera: bool = True) -> Dict:
    """
    Determines when each data source begins for a gauge and the earliest fully-aligned dates.

    Useful for choosing a training start date per river: before "aligned_tabular_begin" at least one
    tabular source is all-null, and before "aligned_multimodal_begin" Sentinel-2 imagery is also
    missing. (Training can still use earlier data with modality masks; these dates mark where the
    record becomes dense.)

    :param site_number: The USGS gauge site number.
    :type site_number: str
    :param discovery: A prior :func:`discover_catchment` result to reuse, defaults to None which runs
        a minimal discovery (no basin, no GAGES-II).
    :type discovery: Dict, optional
    :param include_sentinel: Whether to look up the tile's earliest Sentinel-2 scene, defaults to True.
    :type include_sentinel: bool, optional
    :param include_camera: Whether to look up the gauge's webcam and its earliest retained image
        (the NIMS bucket only keeps ~15 rolling months), defaults to True.
    :type include_camera: bool, optional
    :return: A dict with per-source begin dates (ISO strings or None) and the derived
        "aligned_tabular_begin" and "aligned_multimodal_begin" dates.
    :rtype: Dict
    """
    if discovery is None:
        discovery = discover_catchment(site_number, include_basin=False)
    static = discovery["static"]
    availability: Dict = {"usgs_hourly_begin": None, "asos_begin": None, "scan_begin": None,
                          "nldas_begin": NLDAS_BEGIN_DATE, "sentinel_begin": None,
                          "camera_begin": None}

    catalog = get_period_of_record(site_number)
    if "uv_00060" in catalog:
        availability["usgs_hourly_begin"] = catalog["uv_00060"]["begin_date"]

    if discovery.get("asos_station") is not None:
        availability["asos_begin"] = discovery["asos_station"].get("archive_begin")
    if discovery.get("scan_station") is not None:
        availability["scan_begin"] = discovery["scan_station"].get("element_begin") or \
            get_element_begin_date(discovery["scan_station"]["stationTriplet"], element_code="SMS")

    if include_sentinel:
        from sentinel_functions import latlon_to_mgrs_tile, list_sentinel_safes
        scenes = list_sentinel_safes(latlon_to_mgrs_tile(static["dec_lat_va"], static["dec_long_va"]))
        if len(scenes) > 0:
            availability["sentinel_begin"] = str(scenes["sensing_time"].iloc[0].date())

    if include_camera:
        from camera_functions import find_camera_prefix, list_camera_images
        camera_prefix = find_camera_prefix(FIPS_TO_STATE[static["state_cd"]], static["station_nm"])
        if camera_prefix is not None:
            images = list_camera_images(camera_prefix)
            if len(images) > 0:
                availability["camera_begin"] = str(images["datetime"].iloc[0].date())

    tabular = [availability["usgs_hourly_begin"], availability["asos_begin"],
               availability["scan_begin"], availability["nldas_begin"]]
    if all(value is not None for value in tabular):
        availability["aligned_tabular_begin"] = max(tabular)
    else:
        availability["aligned_tabular_begin"] = None
    if availability["aligned_tabular_begin"] is not None and availability["sentinel_begin"] is not None:
        availability["aligned_multimodal_begin"] = max(availability["aligned_tabular_begin"],
                                                      availability["sentinel_begin"])
    else:
        availability["aligned_multimodal_begin"] = None
    return availability


def fetch_hourly_chunk(site_number: str, start_time: datetime, end_time: datetime, discovery: Dict,
                       include_nldas: bool = False, earthdata_token: Optional[str] = None,
                       max_scan_distance_km: float = 75.0) -> pd.DataFrame:
    """
    Fetches and joins one time window of hourly data using a prior :func:`discover_catchment` result.

    :param site_number: The USGS gauge site number.
    :type site_number: str
    :param start_time: The start of the window.
    :type start_time: datetime
    :param end_time: The end of the window.
    :type end_time: datetime
    :param discovery: The discovery dict from :func:`discover_catchment`.
    :type discovery: Dict
    :param include_nldas: Whether to join NLDAS-2 forcing, defaults to False.
    :type include_nldas: bool, optional
    :param earthdata_token: Earthdata token for NLDAS, defaults to None (reads EARTHDATA_TOKEN).
    :type earthdata_token: str, optional
    :param max_scan_distance_km: Skip the soil moisture join beyond this distance, defaults to 75.0.
    :type max_scan_distance_km: float, optional
    :return: The joined hourly dataframe for the window (may be empty when the gauge has no data).
    :rtype: pd.DataFrame
    """
    static = discovery["static"]
    hourly = usgs_to_hourly_utc(rename_cols(make_usgs_data(start_time, end_time, site_number)))
    if hourly.empty:
        return hourly

    if discovery.get("asos_station") is not None:
        asos = get_hourly_asos(discovery["asos_station"]["station_id"], start_time, end_time)
        if len(asos) > 0:
            hourly = hourly.merge(asos, on="datetime", how="left")

    scan_station = discovery.get("scan_station")
    if scan_station is not None and scan_station["distance_km"] <= max_scan_distance_km:
        soil = get_scan_soil_moisture(scan_station["stationTriplet"], start_time, end_time,
                                      utc_offset_hours=scan_station.get("dataTimeZone"))
        if len(soil) > 0:
            hourly = hourly.merge(soil, on="datetime", how="left")

    if include_nldas:
        forcing = get_nldas_forcing(static["dec_lat_va"], static["dec_long_va"], start_time, end_time,
                                    token=earthdata_token)
        if "potential_evaporation" in forcing.columns:
            # NLDAS-2 PotEvap is an hourly accumulation in kg/m^2, which equals mm; negative values
            # (condensation) are clipped since GR4 expects a non-negative PET forcing.
            forcing["pet_mm_hr"] = forcing["potential_evaporation"].clip(lower=0.0)
        hourly = hourly.merge(forcing, on="datetime", how="left")
    return hourly


def build_catchment_bundle(site_number: str, start_time: datetime, end_time: datetime,
                           output_dir: str = ".", include_soil_moisture: bool = True,
                           max_scan_distance_km: float = 75.0, include_basin: bool = True,
                           include_asos: bool = True,
                           include_nldas: bool = False,
                           earthdata_token: Optional[str] = None,
                           gages2_zip_path: Optional[str] = None) -> Dict:
    """
    Builds and writes the full data bundle for one USGS gauge.

    :param site_number: The USGS gauge site number, e.g. "06752260".
    :type site_number: str
    :param start_time: The start of the requested period.
    :type start_time: datetime
    :param end_time: The end of the requested period.
    :type end_time: datetime
    :param output_dir: Directory where the three bundle artifacts are written, defaults to ".".
    :type output_dir: str, optional
    :param include_soil_moisture: Whether to join soil moisture from the nearest SCAN station,
        defaults to True.
    :type include_soil_moisture: bool, optional
    :param max_scan_distance_km: Skip the soil moisture join when the nearest SCAN station is farther
        than this many km from the gauge, defaults to 75.0.
    :type max_scan_distance_km: float, optional
    :param include_basin: Whether to fetch the NLDI basin polygon (some gauges are not indexed by NLDI;
        set False for those), defaults to True.
    :type include_basin: bool, optional
    :param include_asos: Whether to join hourly surface observations (tmpf, p01m, wind, ...) from the
        nearest ASOS station in the gauge's state, defaults to True. ASOS provides actual point
        measurements complementing the gridded NLDAS-2 forcing.
    :type include_asos: bool, optional
    :param include_nldas: Whether to join NLDAS-2 forcing via Giovanni (requires an Earthdata token),
        defaults to False.
    :type include_nldas: bool, optional
    :param earthdata_token: An Earthdata bearer token for NLDAS, defaults to None which reads the
        EARTHDATA_TOKEN environment variable.
    :type earthdata_token: str, optional
    :param gages2_zip_path: Path to (or for) the GAGES-II archive; when provided, GAGES-II basin
        attributes (topography, soils, climate, geology, hydrology) are merged into the static dict
        with a "gages2\\_" prefix, downloading the archive to this path first if missing. Defaults to
        None which skips the join.
    :type gages2_zip_path: str, optional
    :return: A dict with keys "static" (attribute dict), "hourly" (the joined dataframe) and
        "basin_geometry" (GeoJSON geometry dict or None).
    :rtype: Dict
    """
    os.makedirs(output_dir, exist_ok=True)
    discovery = discover_catchment(site_number, include_basin=include_basin,
                                   include_asos=include_asos,
                                   include_soil_moisture=include_soil_moisture,
                                   gages2_zip_path=gages2_zip_path)
    static = discovery["static"]
    basin_geometry = discovery["basin_geometry"]
    if basin_geometry is not None:
        with open(os.path.join(output_dir, site_number + "_basin.geojson"), "w") as f:
            json.dump({"type": "Feature", "geometry": basin_geometry,
                       "properties": {"site_no": site_number}}, f)

    hourly = fetch_hourly_chunk(site_number, start_time, end_time, discovery,
                                include_nldas=include_nldas, earthdata_token=earthdata_token,
                                max_scan_distance_km=max_scan_distance_km)

    hourly.to_csv(os.path.join(output_dir, site_number + "_hourly.csv"), index=False)
    with open(os.path.join(output_dir, site_number + "_static.json"), "w") as f:
        json.dump(static, f, default=str)
    return {"static": static, "hourly": hourly, "basin_geometry": basin_geometry}
