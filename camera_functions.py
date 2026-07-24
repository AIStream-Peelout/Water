"""
Functions for archiving USGS NIMS river webcam images from the public ``usgs-nims-images`` S3 bucket.

The bucket is a **rolling archive**: images older than roughly 15 months are deleted (verified July 2026:
earliest retained Nathrop image is 2025-04-16), so historical backfill is impossible — cameras must be
archived continuously, which is why this module exists separately from any one training window. Images
arrive about every 15 minutes per camera and are named
``overlay/<CAMERA_ID>/<CAMERA_ID>___<YYYY-MM-DDTHH-MM-SSZ>.jpg`` with UTC timestamps.

Not every gauge has a camera; use :func:`find_camera_prefix` to look one up from NWIS metadata and treat
"no camera" as a normal outcome. This module keeps to light dependencies (anonymous boto3, pandas) so it
can run anywhere; the heavier ``HydroScraper.scrape_images`` remains for the BigQuery-integrated flow.
"""
import os
import re
import time
from datetime import datetime, timezone
from typing import List, Optional

import boto3
import pandas as pd
from botocore import UNSIGNED
from botocore.config import Config

NIMS_BUCKET = "usgs-nims-images"
IMAGE_KEY_PATTERN = re.compile(r"___(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})Z\.jpg$")


def _anonymous_s3_client():
    """
    Creates an anonymous S3 client for the public NIMS bucket.

    :return: A boto3 S3 client with unsigned requests.
    :rtype: botocore.client.BaseClient
    """
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def list_cameras(state_abbrev: Optional[str] = None) -> List[str]:
    """
    Lists the camera ids available in the NIMS bucket.

    :param state_abbrev: Optional two-letter state prefix to filter by (camera ids start with the
        state, e.g. "CO_Clear_Creek_at_Golden"), defaults to None for all cameras.
    :type state_abbrev: str, optional
    :return: The camera ids (without the "overlay/" prefix or trailing slash).
    :rtype: List[str]
    """
    client = _anonymous_s3_client()
    prefix = "overlay/" + (state_abbrev + "_" if state_abbrev else "")
    cameras: List[str] = []
    token: Optional[str] = None
    while True:
        kwargs = {"Bucket": NIMS_BUCKET, "Prefix": prefix, "Delimiter": "/"}
        if token:
            kwargs["ContinuationToken"] = token
        response = client.list_objects_v2(**kwargs)
        for entry in response.get("CommonPrefixes", []):
            cameras.append(entry["Prefix"].split("/")[1])
        if not response.get("IsTruncated"):
            return cameras
        token = response.get("NextContinuationToken")


def find_camera_prefix(state_abbrev: str, station_name: str) -> Optional[str]:
    """
    Finds the camera prefix matching a gauge's NWIS station name, if that gauge has a camera.

    Comparison is done on lowercased, underscore-normalized names, so the NWIS style
    ("ARKANSAS RIVER NEAR NATHROP, CO.") matches the camera style ("CO_Arkansas_River_near_Nathrop").

    :param state_abbrev: The two-letter state abbreviation, e.g. "CO".
    :type state_abbrev: str
    :param station_name: The NWIS station name (station_nm from the site service).
    :type station_name: str
    :return: The bucket prefix "overlay/<camera_id>/" or None when the gauge has no camera.
    :rtype: str, optional
    """
    normalized = station_name.lower().split(",")[0]
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")
    target = state_abbrev.lower() + "_" + normalized
    for camera_id in list_cameras(state_abbrev):
        if camera_id.lower() == target:
            return "overlay/" + camera_id + "/"
    return None


def list_camera_images(camera_prefix: str, start_time: Optional[datetime] = None,
                       end_time: Optional[datetime] = None) -> pd.DataFrame:
    """
    Lists the retained images of a camera, optionally restricted to a UTC time range.

    :param camera_prefix: The camera prefix, e.g. "overlay/CO_Arkansas_River_near_Nathrop/".
    :type camera_prefix: str
    :param start_time: Earliest image time to keep (UTC), defaults to None.
    :type start_time: datetime, optional
    :param end_time: Latest image time to keep (UTC), defaults to None.
    :type end_time: datetime, optional
    :return: A dataframe sorted by time with "datetime" (tz-aware UTC) and "key" columns.
    :rtype: pd.DataFrame
    """
    client = _anonymous_s3_client()
    records = []
    token: Optional[str] = None
    while True:
        kwargs = {"Bucket": NIMS_BUCKET, "Prefix": camera_prefix}
        if token:
            kwargs["ContinuationToken"] = token
        response = client.list_objects_v2(**kwargs)
        for obj in response.get("Contents", []):
            match = IMAGE_KEY_PATTERN.search(obj["Key"])
            if match is None:
                continue
            stamp = datetime.strptime(match.group(1), "%Y-%m-%dT%H-%M-%S").replace(tzinfo=timezone.utc)
            records.append({"datetime": stamp, "key": obj["Key"]})
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")
    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=["datetime", "key"])
    if start_time is not None:
        df = df[df["datetime"] >= pd.Timestamp(start_time, tz="UTC")]
    if end_time is not None:
        df = df[df["datetime"] <= pd.Timestamp(end_time, tz="UTC")]
    return df.sort_values("datetime").reset_index(drop=True)


def download_camera_images(camera_prefix: str, output_dir: str, start_time: datetime,
                           end_time: datetime, min_interval_minutes: float = 60.0,
                           max_images: Optional[int] = None) -> pd.DataFrame:
    """
    Downloads a time-subsampled image archive for a camera and writes a manifest CSV.

    Cameras record roughly every 15 minutes; ``min_interval_minutes`` thins that to a sustainable
    archive cadence (60 keeps ~720 images/month at ~100 KB each).

    :param camera_prefix: The camera prefix, e.g. "overlay/CO_Arkansas_River_near_Nathrop/".
    :type camera_prefix: str
    :param output_dir: Directory for the .jpg files and "camera_manifest.csv".
    :type output_dir: str
    :param start_time: Earliest image time to download (UTC).
    :type start_time: datetime
    :param end_time: Latest image time to download (UTC).
    :type end_time: datetime
    :param min_interval_minutes: Minimum spacing between kept images, defaults to 60.0.
    :type min_interval_minutes: float, optional
    :param max_images: Stop after this many downloads (useful for tests), defaults to None.
    :type max_images: int, optional
    :return: The manifest dataframe with "datetime", "key" and "image_path" columns.
    :rtype: pd.DataFrame
    """
    os.makedirs(output_dir, exist_ok=True)
    listing = list_camera_images(camera_prefix, start_time, end_time)
    client = _anonymous_s3_client()
    camera_id = camera_prefix.rstrip("/").split("/")[-1]
    records = []
    last_kept: Optional[pd.Timestamp] = None
    for _, row in listing.iterrows():
        if last_kept is not None and (row["datetime"] - last_kept) < pd.Timedelta(minutes=min_interval_minutes):
            continue
        local_path = os.path.join(output_dir,
                                  camera_id + "_" + row["datetime"].strftime("%Y%m%d_%H%M%S") + ".jpg")
        client.download_file(NIMS_BUCKET, row["key"], local_path)
        records.append({"datetime": row["datetime"], "key": row["key"], "image_path": local_path})
        last_kept = row["datetime"]
        time.sleep(0.05)
        if max_images is not None and len(records) >= max_images:
            break
    manifest = pd.DataFrame(records)
    manifest.to_csv(os.path.join(output_dir, "camera_manifest.csv"), index=False)
    return manifest
