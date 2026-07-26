"""
Builds the v0 catchment *embedding* dataset: one record per gauge for contrastive pretraining.

Unlike the full hourly scrape, the embedding module only needs a catchment's identity signature, so a
record is deliberately light and fast to collect: one clear-sky Sentinel-2 patch, the GAGES-II static
attribute vector, and a multi-year **daily** flow history (one NWIS ``dv`` request per gauge instead
of hundreds of iv chunks). This lets a whole state be collected in hours, in parallel with the hourly
fleet scrape, so embedding pretraining and representation analysis can start immediately.

Per gauge, writes ``<site>.npz`` containing:

* ``image``      — float32 (bands, patch, patch) Sentinel-2 patch (clearest scene of a summer window)
* ``history``    — float32 (n_days,) daily mean discharge in cfs (NaN where missing)
* ``history_start`` — ISO date of the first history value
* ``static``     — float32 vector of numeric GAGES-II attributes
* ``static_names`` — the attribute names, aligned with ``static``

plus a state-level ``manifest.csv`` marking success/skip reasons per gauge.

Example::

    python embedding_dataset.py --state CO
"""
import argparse
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

from gages2_functions import DEFAULT_TABLES, download_gages2, gauge_in_gages2, load_gages2_table
from sentinel_functions import extract_patch, get_cloud_cover, latlon_to_mgrs_tile, list_sentinel_safes
from state_scrape import list_state_gauges

DAILY_VALUES_URL = ("https://waterservices.usgs.gov/nwis/dv/?format=json&sites={}&parameterCd=00060"
                    "&startDT={}&endDT={}")


def get_daily_flow(site_number: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
    """
    Fetches mean daily discharge for a gauge from the NWIS daily-values service.

    :param site_number: The USGS gauge site number.
    :type site_number: str
    :param start_time: The start of the requested period.
    :type start_time: datetime
    :param end_time: The end of the requested period.
    :type end_time: datetime
    :return: A dataframe with "date" and "cfs" columns (empty when the gauge has no daily record).
    :rtype: pd.DataFrame
    """
    url = DAILY_VALUES_URL.format(site_number, start_time.strftime("%Y-%m-%d"),
                                  end_time.strftime("%Y-%m-%d"))
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    series = response.json()["value"]["timeSeries"]
    if not series:
        return pd.DataFrame(columns=["date", "cfs"])
    values = series[0]["values"][0]["value"]
    df = pd.DataFrame({"date": [v["dateTime"][:10] for v in values],
                       "cfs": [float(v["value"]) for v in values]})
    no_data = series[0]["variable"].get("noDataValue")
    if no_data is not None:
        df.loc[df["cfs"] == no_data, "cfs"] = np.nan
    return df


def build_static_matrix(zip_path: str) -> Tuple[pd.DataFrame, List[str]]:
    """
    Builds the numeric GAGES-II attribute matrix across all gauges (loaded once per state run).

    :param zip_path: Path to the GAGES-II archive.
    :type zip_path: str
    :return: A tuple of (dataframe indexed by STAID with numeric attribute columns, column names).
    :rtype: Tuple[pd.DataFrame, List[str]]
    """
    merged: Optional[pd.DataFrame] = None
    for table_name in DEFAULT_TABLES:
        table = load_gages2_table(table_name, zip_path).set_index("STAID")
        numeric = table.select_dtypes(include=[np.number])
        merged = numeric if merged is None else merged.join(numeric, how="outer", rsuffix="_dup")
    return merged, list(merged.columns)


def collect_gauge_record(site_number: str, latitude: float, longitude: float,
                         static_matrix: pd.DataFrame, output_dir: str,
                         history_start: datetime, history_end: datetime,
                         scene_window: Tuple[datetime, datetime], patch_size: int = 128,
                         bands: Tuple[str, ...] = ("B02", "B03", "B04", "B08"),
                         min_history_days: int = 1460) -> Dict:
    """
    Collects and writes the embedding record of one gauge.

    :param site_number: The USGS gauge site number.
    :type site_number: str
    :param latitude: Gauge latitude in decimal degrees.
    :type latitude: float
    :param longitude: Gauge longitude in decimal degrees.
    :type longitude: float
    :param static_matrix: The GAGES-II numeric matrix from :func:`build_static_matrix`.
    :type static_matrix: pd.DataFrame
    :param output_dir: Directory for the .npz records.
    :type output_dir: str
    :param history_start: Start of the daily-flow history window.
    :type history_start: datetime
    :param history_end: End of the daily-flow history window.
    :type history_end: datetime
    :param scene_window: (start, end) window in which to pick the clearest Sentinel scene.
    :type scene_window: Tuple[datetime, datetime]
    :param patch_size: The image patch size in 10 m pixels, defaults to 128.
    :type patch_size: int, optional
    :param bands: The Sentinel bands to extract, defaults to the 10 m bands.
    :type bands: Tuple[str, ...], optional
    :param min_history_days: Skip gauges with fewer daily-flow values, defaults to 1460 (4 years).
    :type min_history_days: int, optional
    :return: A manifest row dict with "status" ("ok" or a skip reason) and metadata.
    :rtype: Dict
    """
    if site_number not in static_matrix.index:
        return {"site_no": site_number, "status": "no_gages2"}
    flow = get_daily_flow(site_number, history_start, history_end)
    if flow["cfs"].notna().sum() < min_history_days:
        return {"site_no": site_number, "status": "short_history",
                "history_days": int(flow["cfs"].notna().sum())}

    scenes = list_sentinel_safes(latlon_to_mgrs_tile(latitude, longitude), scene_window[0],
                                 scene_window[1])
    if scenes.empty:
        return {"site_no": site_number, "status": "no_sentinel_scenes"}
    scenes = scenes.assign(cloud=scenes["safe_prefix"].map(get_cloud_cover)).sort_values("cloud")
    patch = None
    for _, scene in scenes.head(3).iterrows():
        candidate = extract_patch(scene["safe_prefix"], latitude, longitude, bands=bands,
                                  patch_size=patch_size)
        if (candidate > 0).any(axis=0).mean() > 0.5:
            patch, chosen = candidate, scene
            break
    if patch is None:
        return {"site_no": site_number, "status": "no_valid_patch"}

    dates = pd.to_datetime(flow["date"])
    full_index = pd.date_range(history_start, history_end, freq="D")
    history = pd.Series(np.nan, index=full_index)
    history.loc[dates] = flow["cfs"].to_numpy()
    static_row = static_matrix.loc[site_number].to_numpy(dtype=np.float32)
    np.savez_compressed(
        os.path.join(output_dir, site_number + ".npz"), image=patch.astype(np.float32),
        history=history.to_numpy(dtype=np.float32),
        history_start=str(full_index[0].date()),
        static=static_row, static_names=np.array(static_matrix.columns, dtype=str))
    return {"site_no": site_number, "status": "ok", "cloud": float(chosen["cloud"]),
            "scene": chosen["product_id"], "history_days": int(flow["cfs"].notna().sum())}


def run_state_collection(state_abbrev: str, output_root: str = os.path.join("pilot_data",
                                                                            "embedding_dataset"),
                         gages2_zip_path: str = os.path.join("pilot_data", "gages2.zip"),
                         history_years: int = 15, limit: Optional[int] = None) -> pd.DataFrame:
    """
    Collects embedding records for every gauge of a state, resumably.

    :param state_abbrev: The two-letter state abbreviation.
    :type state_abbrev: str
    :param output_root: Root output directory, defaults to pilot_data/embedding_dataset.
    :type output_root: str, optional
    :param gages2_zip_path: The GAGES-II archive path, defaults to pilot_data/gages2.zip.
    :type gages2_zip_path: str, optional
    :param history_years: Length of the daily-flow history window ending today, defaults to 15.
    :type history_years: int, optional
    :param limit: Stop after this many new records, defaults to None.
    :type limit: int, optional
    :return: The manifest dataframe.
    :rtype: pd.DataFrame
    """
    state_dir = os.path.join(output_root, state_abbrev)
    os.makedirs(state_dir, exist_ok=True)
    download_gages2(gages2_zip_path)
    static_matrix, _ = build_static_matrix(gages2_zip_path)
    gauges = list_state_gauges(state_abbrev)
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    history_start = today.replace(year=today.year - history_years)
    scene_window = (datetime(today.year - 1, 6, 1), datetime(today.year - 1, 10, 1))

    manifest_path = os.path.join(state_dir, "manifest.csv")
    manifest: List[Dict] = pd.read_csv(manifest_path).to_dict("records") \
        if os.path.exists(manifest_path) else []
    done = {row["site_no"] for row in manifest}
    collected = 0
    for _, gauge in gauges.iterrows():
        site = str(gauge["site_no"])
        if site in done:
            continue
        if limit is not None and collected >= limit:
            break
        try:
            row = collect_gauge_record(site, float(gauge["dec_lat_va"]), float(gauge["dec_long_va"]),
                                       static_matrix, state_dir, history_start, today, scene_window)
        except Exception as error:  # noqa: BLE001 - one bad gauge must not stop the fleet
            row = {"site_no": site, "status": "error", "error": str(error)[:200]}
        row["station_nm"] = gauge["station_nm"]
        manifest.append(row)
        collected += 1
        print("[%d] %s -> %s" % (collected, site, row["status"]))
        pd.DataFrame(manifest).to_csv(manifest_path, index=False)
    return pd.DataFrame(manifest)


def main() -> None:
    """
    CLI entry point for the embedding dataset collection.

    :return: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Collect the v0 catchment embedding dataset.")
    parser.add_argument("--state", required=True, help="Two-letter state abbreviation")
    parser.add_argument("--limit", type=int, default=None, help="Max new records this run")
    parser.add_argument("--history-years", type=int, default=15)
    args = parser.parse_args()
    manifest = run_state_collection(args.state, history_years=args.history_years, limit=args.limit)
    print(json.dumps(manifest["status"].value_counts().to_dict(), indent=2))


if __name__ == "__main__":
    main()
