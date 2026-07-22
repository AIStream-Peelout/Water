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

from awdb_functions import find_nearest_awdb_station, get_scan_soil_moisture
from nldas_functions import get_nldas_forcing
from scrape_text import timezone_map
from usgs_scraping_functions import (basin_bounding_box, get_basin_boundary, get_site_metadata,
                                     make_usgs_data, rename_cols)



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


def build_catchment_bundle(site_number: str, start_time: datetime, end_time: datetime,
                           output_dir: str = ".", include_soil_moisture: bool = True,
                           max_scan_distance_km: float = 75.0, include_basin: bool = True,
                           include_nldas: bool = False,
                           earthdata_token: Optional[str] = None) -> Dict:
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
    :param include_nldas: Whether to join NLDAS-2 forcing via Giovanni (requires an Earthdata token),
        defaults to False.
    :type include_nldas: bool, optional
    :param earthdata_token: An Earthdata bearer token for NLDAS, defaults to None which reads the
        EARTHDATA_TOKEN environment variable.
    :type earthdata_token: str, optional
    :return: A dict with keys "static" (attribute dict), "hourly" (the joined dataframe) and
        "basin_geometry" (GeoJSON geometry dict or None).
    :rtype: Dict
    """
    os.makedirs(output_dir, exist_ok=True)
    static = get_site_metadata(site_number)
    latitude, longitude = static["dec_lat_va"], static["dec_long_va"]

    basin_geometry = None
    if include_basin:
        basin_geometry = get_basin_boundary(site_number)
        static["basin_bbox"] = basin_bounding_box(basin_geometry, buffer_degrees=0.05)
        with open(os.path.join(output_dir, site_number + "_basin.geojson"), "w") as f:
            json.dump({"type": "Feature", "geometry": basin_geometry,
                       "properties": {"site_no": site_number}}, f)

    hourly = usgs_to_hourly_utc(rename_cols(make_usgs_data(start_time, end_time, site_number)))

    if include_soil_moisture:
        scan_station = find_nearest_awdb_station(latitude, longitude, network="SCAN")
        static["scan_triplet"] = scan_station["stationTriplet"]
        static["scan_distance_km"] = scan_station["distance_km"]
        if scan_station["distance_km"] <= max_scan_distance_km:
            soil = get_scan_soil_moisture(scan_station["stationTriplet"], start_time, end_time,
                                          utc_offset_hours=scan_station.get("dataTimeZone"))
            if len(soil) > 0:
                hourly = hourly.merge(soil, on="datetime", how="left")

    if include_nldas:
        forcing = get_nldas_forcing(latitude, longitude, start_time, end_time, token=earthdata_token)
        if "potential_evaporation" in forcing.columns:
            # NLDAS-2 PotEvap is an hourly accumulation in kg/m^2, which equals mm; negative values
            # (condensation) are clipped since GR4 expects a non-negative PET forcing.
            forcing["pet_mm_hr"] = forcing["potential_evaporation"].clip(lower=0.0)
        hourly = hourly.merge(forcing, on="datetime", how="left")

    hourly.to_csv(os.path.join(output_dir, site_number + "_hourly.csv"), index=False)
    with open(os.path.join(output_dir, site_number + "_static.json"), "w") as f:
        json.dump(static, f, default=str)
    return {"static": static, "hourly": hourly, "basin_geometry": basin_geometry}
