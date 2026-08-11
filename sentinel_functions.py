"""
Functions for extracting Sentinel-2 image patches around USGS gauges from the public GCS bucket.

Scenes are discovered by anonymously listing ``gs://gcp-public-data-sentinel-2`` (no credentials needed)
and pixel windows are streamed straight out of the cloud-hosted JP2s with rasterio's ``/vsicurl/`` driver,
so a patch costs a few MB of range requests instead of a ~700 MB SAFE download. Opening a remote JP2
costs roughly 20 seconds in codestream header requests, so patch extraction is a batch operation:
budget ``~20s x n_bands`` per scene and parallelize across scenes when scraping many catchments.

Notes learned from the bucket itself:

* Granule directory names inside a .SAFE prefix are not constructible from the product id — they must be
  discovered by listing (see :func:`find_granule_prefix`).
* Some scenes only partially cover their MGRS tile (orbit edges), so patches can be fully or partly
  empty; :func:`build_patch_time_series` records a ``valid_fraction`` per scene and can skip empty ones.
"""
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

GCS_LIST_URL = "https://storage.googleapis.com/storage/v1/b/gcp-public-data-sentinel-2/o"
GCS_HTTP_BASE = "https://storage.googleapis.com/gcp-public-data-sentinel-2/"

# Ground resolution in meters of each Sentinel-2 L1C band.
BAND_RESOLUTION_M = {
    "B01": 60, "B02": 10, "B03": 10, "B04": 10, "B05": 20, "B06": 20, "B07": 20,
    "B08": 10, "B8A": 20, "B09": 60, "B10": 60, "B11": 20, "B12": 20, "TCI": 10,
}

# GDAL settings that keep /vsicurl/ JP2 access to targeted range requests.
RASTERIO_GCS_ENV = {
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".jp2",
    "GDAL_INGESTED_BYTES_AT_OPEN": "200000",
    "GDAL_HTTP_MERGE_CONSECUTIVE_RANGES": "YES",
}

PRODUCT_ID_PATTERN = re.compile(
    r"(?P<satellite>S2[ABC])_MSIL1C_(?P<sensing>\d{8}T\d{6})_N\d{4}_R(?P<orbit>\d{3})_T(?P<tile>[0-9A-Z]{5})_\d{8}T\d{6}")


def latlon_to_mgrs_tile(latitude: float, longitude: float) -> str:
    """
    Converts a point to its 5-character MGRS tile id (the Sentinel-2 tiling grid).

    :param latitude: The latitude in decimal degrees.
    :type latitude: float
    :param longitude: The longitude in decimal degrees.
    :type longitude: float
    :return: The MGRS tile, e.g. "13TDE".
    :rtype: str
    """
    import mgrs
    return mgrs.MGRS().toMGRS(latitude, longitude, MGRSPrecision=0)


def list_gcs_prefixes(prefix: str) -> List[str]:
    """
    Anonymously lists "subdirectories" under a prefix of the public Sentinel-2 bucket.

    :param prefix: The object prefix to list, e.g. "tiles/13/T/DE/".
    :type prefix: str
    :return: The list of matching prefixes (each ending with "/").
    :rtype: List[str]
    """
    prefixes: List[str] = []
    page_token: Optional[str] = None
    while True:
        params = {"prefix": prefix, "delimiter": "/"}
        if page_token:
            params["pageToken"] = page_token
        response = requests.get(GCS_LIST_URL, params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()
        prefixes.extend(payload.get("prefixes", []))
        page_token = payload.get("nextPageToken")
        if not page_token:
            return prefixes


def parse_product_id(product_id: str) -> Optional[Dict]:
    """
    Parses a Sentinel-2 L1C product id into its components.

    :param product_id: The product id, e.g.
        "S2B_MSIL1C_20240609T174909_N0510_R141_T13TDE_20240609T205810".
    :type product_id: str
    :return: A dict with "satellite", "sensing_time" (tz-aware UTC datetime), "orbit" and "tile" keys,
        or None when the string is not an L1C product id.
    :rtype: Dict, optional
    """
    match = PRODUCT_ID_PATTERN.search(product_id)
    if match is None:
        return None
    sensing = datetime.strptime(match.group("sensing"), "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    return {"satellite": match.group("satellite"), "sensing_time": sensing,
            "orbit": match.group("orbit"), "tile": match.group("tile")}


def list_sentinel_safes(tile: str, start_time: Optional[datetime] = None,
                        end_time: Optional[datetime] = None) -> pd.DataFrame:
    """
    Lists the L1C scenes available for an MGRS tile, optionally restricted to a time range.

    :param tile: The MGRS tile, e.g. "13TDE".
    :type tile: str
    :param start_time: Earliest sensing time to keep (assumed UTC), defaults to None.
    :type start_time: datetime, optional
    :param end_time: Latest sensing time to keep (assumed UTC), defaults to None.
    :type end_time: datetime, optional
    :return: A dataframe sorted by sensing_time with safe_prefix, product_id, satellite, orbit and
        sensing_time (tz-aware UTC) columns.
    :rtype: pd.DataFrame
    """
    tile_prefix = "tiles/" + tile[:2].lstrip("0") + "/" + tile[2] + "/" + tile[3:] + "/"
    records = []
    for safe_prefix in list_gcs_prefixes(tile_prefix):
        parsed = parse_product_id(safe_prefix)
        if parsed is None:
            continue
        parsed["safe_prefix"] = safe_prefix
        parsed["product_id"] = safe_prefix.rstrip("/").split("/")[-1].replace(".SAFE", "")
        records.append(parsed)
    df = pd.DataFrame(records)
    if df.empty:
        return df
    if start_time is not None:
        df = df[df["sensing_time"] >= pd.Timestamp(start_time, tz="UTC")]
    if end_time is not None:
        df = df[df["sensing_time"] <= pd.Timestamp(end_time, tz="UTC")]
    return df.sort_values("sensing_time").reset_index(drop=True)


def find_granule_prefix(safe_prefix: str) -> str:
    """
    Discovers the single granule directory inside a .SAFE prefix.

    :param safe_prefix: The SAFE prefix as returned by :func:`list_sentinel_safes`.
    :type safe_prefix: str
    :return: The granule prefix ending with "/".
    :rtype: str
    """
    granules = list_gcs_prefixes(safe_prefix + "GRANULE/")
    if not granules:
        raise ValueError("No granule found under " + safe_prefix)
    return granules[0]


def get_cloud_cover(safe_prefix: str) -> float:
    """
    Reads the scene-level cloud coverage percentage from the product metadata XML.

    :param safe_prefix: The SAFE prefix of the scene.
    :type safe_prefix: str
    :return: The cloud coverage assessment in percent (0-100).
    :rtype: float
    """
    response = requests.get(GCS_HTTP_BASE + safe_prefix + "MTD_MSIL1C.xml", timeout=60)
    response.raise_for_status()
    match = re.search(r"<Cloud_Coverage_Assessment>([\d.]+)</Cloud_Coverage_Assessment>", response.text)
    if match is None:
        raise ValueError("No Cloud_Coverage_Assessment in metadata for " + safe_prefix)
    return float(match.group(1))


def extract_patch(safe_prefix: str, latitude: float, longitude: float,
                  bands: Tuple[str, ...] = ("B02", "B03", "B04", "B08"), patch_size: int = 128,
                  granule_prefix: Optional[str] = None) -> np.ndarray:
    """
    Streams a multi-band patch centered on a point out of a cloud-hosted scene.

    All bands are resampled onto the 10 m grid of the patch, so the output is aligned across bands
    regardless of their native resolution. Pixels outside the scene footprint are zero.

    :param safe_prefix: The SAFE prefix of the scene.
    :type safe_prefix: str
    :param latitude: The latitude of the patch center in decimal degrees.
    :type latitude: float
    :param longitude: The longitude of the patch center in decimal degrees.
    :type longitude: float
    :param bands: The band names to read, defaults to the 10 m bands ("B02", "B03", "B04", "B08").
    :type bands: Tuple[str, ...], optional
    :param patch_size: The patch width/height in 10 m pixels (128 -> 1.28 km square), defaults to 128.
    :type patch_size: int, optional
    :param granule_prefix: The granule prefix if already discovered, defaults to None which lists it.
    :type granule_prefix: str, optional
    :return: A float32 array of shape (len(bands), patch_size, patch_size).
    :rtype: np.ndarray
    """
    import rasterio
    from rasterio.warp import transform as warp_transform
    from rasterio.windows import Window
    from rasterio.enums import Resampling

    if granule_prefix is None:
        granule_prefix = find_granule_prefix(safe_prefix)
    granule_name = granule_prefix.rstrip("/").split("/")[-1]
    # Band files are named like T13TDE_20240609T174909_B04.jp2 where the timestamp is the *datatake*
    # sensing time from the product id, not the granule discriminator.
    product_sensing = re.search(r"MSIL1C_(\d{8}T\d{6})", safe_prefix).group(1)
    tile_id = granule_name.split("_")[1]
    patch = np.zeros((len(bands), patch_size, patch_size), dtype=np.float32)
    with rasterio.Env(**RASTERIO_GCS_ENV):
        for i, band in enumerate(bands):
            resolution = BAND_RESOLUTION_M[band]
            url = ("/vsicurl/" + GCS_HTTP_BASE + granule_prefix + "IMG_DATA/" +
                   tile_id + "_" + product_sensing + "_" + band + ".jp2")
            with rasterio.open(url) as src:
                xs, ys = warp_transform("EPSG:4326", src.crs, [longitude], [latitude])
                row, col = src.index(xs[0], ys[0])
                native_size = patch_size * 10 // resolution
                window = Window(col - native_size // 2, row - native_size // 2,
                                native_size, native_size)
                patch[i] = src.read(1, window=window, boundless=True, fill_value=0,
                                    out_shape=(patch_size, patch_size),
                                    resampling=Resampling.nearest).astype(np.float32)
    return patch


def build_patch_time_series(latitude: float, longitude: float, start_time: datetime,
                            end_time: datetime, output_dir: str,
                            bands: Tuple[str, ...] = ("B02", "B03", "B04", "B08"),
                            patch_size: int = 128, tile: Optional[str] = None,
                            max_cloud_percent: Optional[float] = None,
                            min_valid_fraction: float = 0.05,
                            max_scenes: Optional[int] = None) -> pd.DataFrame:
    """
    Extracts a patch for every qualifying scene over a period and writes a manifest.

    Patches are saved as ``<tile>_<sensing>.npy`` arrays of shape (bands, patch_size, patch_size) and the
    manifest (also written as ``sentinel_manifest.csv`` in output_dir) records sensing time, cloud cover
    and the fraction of valid (non-zero) pixels, ready to merge with an hourly dataframe on the rounded
    hour like the webcam images in ``scraping_functions``.

    :param latitude: The latitude of the patch center (e.g. the gauge) in decimal degrees.
    :type latitude: float
    :param longitude: The longitude of the patch center in decimal degrees.
    :type longitude: float
    :param start_time: Earliest sensing time to include.
    :type start_time: datetime
    :param end_time: Latest sensing time to include.
    :type end_time: datetime
    :param output_dir: Directory for the .npy patches and the manifest CSV.
    :type output_dir: str
    :param bands: The band names to extract, defaults to the 10 m bands.
    :type bands: Tuple[str, ...], optional
    :param patch_size: The patch width/height in 10 m pixels, defaults to 128.
    :type patch_size: int, optional
    :param tile: The MGRS tile, defaults to None which derives it from the coordinates.
    :type tile: str, optional
    :param max_cloud_percent: Skip scenes cloudier than this percentage, defaults to None (keep all).
    :type max_cloud_percent: float, optional
    :param min_valid_fraction: Skip patches with fewer valid (non-zero) pixels than this fraction
        (partial-coverage orbits), defaults to 0.05.
    :type min_valid_fraction: float, optional
    :param max_scenes: Stop after this many extracted patches (useful for pilots and tests),
        defaults to None.
    :type max_scenes: int, optional
    :return: The manifest dataframe with product_id, sensing_time, cloud_percent, valid_fraction and
        patch_path columns.
    :rtype: pd.DataFrame
    """
    import os
    os.makedirs(output_dir, exist_ok=True)
    if tile is None:
        tile = latlon_to_mgrs_tile(latitude, longitude)
    scenes = list_sentinel_safes(tile, start_time, end_time)
    records = []
    for _, scene in scenes.iterrows():
        cloud_percent = get_cloud_cover(scene["safe_prefix"])
        if max_cloud_percent is not None and cloud_percent > max_cloud_percent:
            continue
        patch = extract_patch(scene["safe_prefix"], latitude, longitude, bands, patch_size)
        valid_fraction = float((patch > 0).any(axis=0).mean())
        if valid_fraction < min_valid_fraction:
            continue
        patch_path = os.path.join(output_dir,
                                  tile + "_" + scene["sensing_time"].strftime("%Y%m%dT%H%M%S") + ".npy")
        np.save(patch_path, patch)
        records.append({"product_id": scene["product_id"], "sensing_time": scene["sensing_time"],
                        "cloud_percent": cloud_percent, "valid_fraction": valid_fraction,
                        "patch_path": patch_path})
        if max_scenes is not None and len(records) >= max_scenes:
            break
    manifest = pd.DataFrame(records)
    manifest.to_csv(os.path.join(output_dir, "sentinel_manifest.csv"), index=False)
    return manifest
