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
from sentinel_functions import (candidate_tiles, extract_patch, footprint_contains,
                                get_scene_metadata, list_sentinel_safes)
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


REGIONAL_BANDS = ("B02", "B03", "B04", "B08", "B11", "B12")
# Seasonal scene windows (month, day) for the regional context patch: the summer scene is
# the primary view, the winter scene a cross-season positive (snow line, leaf-off terrain).
REGIONAL_WINDOWS = {"summer": ((6, 1), (10, 1)), "winter": ((1, 1), (4, 1))}


def covering_scenes(tile: str, latitude: float, longitude: float,
                    scene_window: Tuple[datetime, datetime]) -> Optional[pd.DataFrame]:
    """
    Lists a tile's scenes in a window whose footprint contains the point, clearest first.

    :param tile: The MGRS tile id.
    :type tile: str
    :param latitude: Point latitude in decimal degrees.
    :type latitude: float
    :param longitude: Point longitude in decimal degrees.
    :type longitude: float
    :param scene_window: (start, end) sensing-time window.
    :type scene_window: Tuple[datetime, datetime]
    :return: The covering scenes with a "cloud" column (possibly empty), or None when the tile
        has no scenes at all in the window.
    :rtype: pd.DataFrame, optional
    """
    scenes = list_sentinel_safes(tile, scene_window[0], scene_window[1])
    if scenes.empty:
        return None
    metadata = [get_scene_metadata(prefix) for prefix in scenes["safe_prefix"]]
    scenes = scenes.assign(
        cloud=[m["cloud"] for m in metadata],
        covers=[footprint_contains(m["footprint"], latitude, longitude) for m in metadata])
    return scenes[scenes["covers"]].sort_values("cloud")


def valid_fraction(patch: np.ndarray) -> float:
    """
    Fraction of patch pixels with data in any band (zero marks nodata outside the scene).

    :param patch: A (bands, height, width) patch.
    :type patch: np.ndarray
    :return: The valid fraction in [0, 1].
    :rtype: float
    """
    return float((patch > 0).any(axis=0).mean())


def select_scene_patch(latitude: float, longitude: float, scene_window: Tuple[datetime, datetime],
                       bands: Tuple[str, ...] = ("B02", "B03", "B04", "B08"),
                       patch_size: int = 128, pixel_meters: float = 10.0,
                       resampling: str = "nearest", max_tries: int = 5,
                       min_valid: float = 0.5, mosaic: bool = False, max_fill: int = 2
                       ) -> Tuple[Optional[np.ndarray], Optional[pd.Series], str]:
    """
    Extracts a patch from the clearest scene in a window that actually covers the point.

    Only scenes whose footprint contains the point are ranked by cloud — orbit-edge slivers
    report ~0% cloud over their sliver, so cloud alone selects misses. Neighbor tiles matter
    near UTM-zone/latitude-band boundaries (see :func:`sentinel_functions.candidate_tiles`).
    Wide windows (regional patches) often cross a scene edge even when the point is covered,
    so the best-covered candidate is kept and, with ``mosaic``, its nodata holes are filled
    from other covering scenes — same-day (same datatake) scenes first, which are seamless.

    :param latitude: Point latitude in decimal degrees.
    :type latitude: float
    :param longitude: Point longitude in decimal degrees.
    :type longitude: float
    :param scene_window: (start, end) sensing-time window.
    :type scene_window: Tuple[datetime, datetime]
    :param bands: Bands to extract, defaults to the 10 m bands.
    :type bands: Tuple[str, ...], optional
    :param patch_size: Output patch size in pixels, defaults to 128.
    :type patch_size: int, optional
    :param pixel_meters: Ground size of one output pixel, defaults to 10.0.
    :type pixel_meters: float, optional
    :param resampling: rasterio resampling method name, defaults to "nearest".
    :type resampling: str, optional
    :param max_tries: Covering scenes to read per tile, defaults to 5.
    :type max_tries: int, optional
    :param min_valid: Minimum valid fraction of the kept patch, defaults to 0.5.
    :type min_valid: float, optional
    :param mosaic: Fill nodata holes from other covering scenes, defaults to False.
    :type mosaic: bool, optional
    :param max_fill: Maximum extra scene reads for the mosaic, defaults to 2.
    :type max_fill: int, optional
    :return: (patch, chosen scene row, "ok"), or (None, None, failure status) where the status
        is "no_sentinel_scenes", "no_covering_scene" or "no_valid_patch".
    :rtype: Tuple[Optional[np.ndarray], Optional[pd.Series], str]
    """
    any_scene, any_covering, best = False, False, None
    tiles = candidate_tiles(latitude, longitude)
    covering_by_tile: Dict[str, Optional[pd.DataFrame]] = {}
    for tile in tiles:
        covering = covering_scenes(tile, latitude, longitude, scene_window)
        covering_by_tile[tile] = covering
        if covering is None:
            continue
        any_scene = True
        if covering.empty:
            continue
        any_covering = True
        for _, scene in covering.head(max_tries).iterrows():
            candidate = extract_patch(scene["safe_prefix"], latitude, longitude, bands=bands,
                                      patch_size=patch_size, pixel_meters=pixel_meters,
                                      resampling=resampling)
            valid = valid_fraction(candidate)
            if best is None or valid > best[2]:
                best = (candidate, scene, valid)
            if valid >= 0.98:
                break
        if best is not None and best[2] >= min_valid:
            break
    if best is None or best[2] < min_valid:
        status = "no_valid_patch" if any_covering else \
            ("no_covering_scene" if any_scene else "no_sentinel_scenes")
        return None, None, status
    patch, scene, valid = best
    fills = 0
    for tile in tiles if mosaic else ():
        if valid >= 0.98 or fills >= max_fill:
            break
        covering = covering_by_tile.get(tile, "unlisted")
        if isinstance(covering, str):
            covering = covering_scenes(tile, latitude, longitude, scene_window)
        if covering is None or covering.empty:
            continue
        others = covering[covering["product_id"] != scene["product_id"]]
        same_day = others["sensing_time"].dt.date == scene["sensing_time"].date()
        for _, other in pd.concat([others[same_day], others[~same_day]]).iterrows():
            if valid >= 0.98 or fills >= max_fill:
                break
            candidate = extract_patch(other["safe_prefix"], latitude, longitude, bands=bands,
                                      patch_size=patch_size, pixel_meters=pixel_meters,
                                      resampling=resampling)
            holes = ~(patch > 0).any(axis=0)
            patch[:, holes] = candidate[:, holes]
            valid, fills = valid_fraction(patch), fills + 1
    return patch, scene, "ok"


def collect_regional_record(site_number: str, latitude: float, longitude: float,
                            output_dir: str, year: int, patch_size: int = 512,
                            pixel_meters: float = 50.0,
                            bands: Tuple[str, ...] = REGIONAL_BANDS) -> Dict:
    """
    Writes the regional-context sidecar ``<site>_regional.npz`` (summer + winter patches).

    The 1.28 km gauge-reach patch of the base record covers ~0.3%% of the median catchment;
    the regional patch (default 512 px at 50 m = 25.6 km) contains the whole catchment for
    most of the fleet, with SWIR bands for snow, open water and soil moisture.

    :param site_number: The USGS gauge site number.
    :type site_number: str
    :param latitude: Gauge latitude in decimal degrees.
    :type latitude: float
    :param longitude: Gauge longitude in decimal degrees.
    :type longitude: float
    :param output_dir: Directory holding the state's records.
    :type output_dir: str
    :param year: Calendar year of the scene windows.
    :type year: int
    :param patch_size: Output patch size in pixels, defaults to 512.
    :type patch_size: int, optional
    :param pixel_meters: Ground size of one output pixel, defaults to 50.0.
    :type pixel_meters: float, optional
    :param bands: Bands to extract, defaults to :data:`REGIONAL_BANDS`.
    :type bands: Tuple[str, ...], optional
    :return: A manifest row dict with "status" ("ok" or "<season>_<failure>") and scene metadata.
    :rtype: Dict
    """
    arrays, row = {}, {"site_no": site_number}
    for season, ((start_month, start_day), (end_month, end_day)) in REGIONAL_WINDOWS.items():
        window = (datetime(year, start_month, start_day), datetime(year, end_month, end_day))
        patch, scene, status = select_scene_patch(latitude, longitude, window, bands=bands,
                                                  patch_size=patch_size,
                                                  pixel_meters=pixel_meters,
                                                  resampling="average", min_valid=0.3,
                                                  mosaic=True)
        if patch is None:
            row["status"] = season + "_" + status
            return row
        arrays["image_regional" if season == "summer" else "image_regional_alt"] = patch
        row[season + "_scene"] = scene["product_id"]
        row[season + "_cloud"] = float(scene["cloud"])
        row[season + "_valid"] = round(valid_fraction(patch), 3)
    np.savez_compressed(os.path.join(output_dir, site_number + "_regional.npz"),
                        bands=np.array(bands, dtype=str), pixel_meters=float(pixel_meters),
                        **arrays)
    row["status"] = "ok"
    return row


def run_regional_collection(state_abbrev: str,
                            output_root: str = os.path.join("pilot_data", "embedding_dataset"),
                            year: Optional[int] = None, limit: Optional[int] = None,
                            patch_size: int = 512, pixel_meters: float = 50.0) -> pd.DataFrame:
    """
    Adds regional-context sidecars for every successfully collected gauge of a state, resumably.

    :param state_abbrev: The two-letter state abbreviation.
    :type state_abbrev: str
    :param output_root: Root output directory, defaults to pilot_data/embedding_dataset.
    :type output_root: str, optional
    :param year: Scene-window year, defaults to None (last calendar year).
    :type year: int, optional
    :param limit: Stop after this many new sidecars, defaults to None.
    :type limit: int, optional
    :param patch_size: Output patch size in pixels, defaults to 512.
    :type patch_size: int, optional
    :param pixel_meters: Ground size of one output pixel, defaults to 50.0.
    :type pixel_meters: float, optional
    :return: The regional manifest dataframe.
    :rtype: pd.DataFrame
    """
    state_dir = os.path.join(output_root, state_abbrev)
    base = pd.read_csv(os.path.join(state_dir, "manifest.csv"), dtype={"site_no": str})
    sites = base[base["status"] == "ok"]["site_no"].tolist()
    gauges = list_state_gauges(state_abbrev).set_index("site_no")
    manifest_path = os.path.join(state_dir, "manifest_regional.csv")
    manifest: List[Dict] = pd.read_csv(manifest_path, dtype={"site_no": str}).to_dict("records") \
        if os.path.exists(manifest_path) else []
    done = {row["site_no"] for row in manifest}
    year = year or datetime.now().year - 1
    collected = 0
    for site in sites:
        if site in done:
            continue
        if limit is not None and collected >= limit:
            break
        if site not in gauges.index:
            row = {"site_no": site, "status": "no_coordinates"}
        else:
            try:
                row = collect_regional_record(site, float(gauges.loc[site, "dec_lat_va"]),
                                              float(gauges.loc[site, "dec_long_va"]), state_dir,
                                              year, patch_size=patch_size,
                                              pixel_meters=pixel_meters)
            except Exception as error:  # noqa: BLE001 - one bad gauge must not stop the fleet
                row = {"site_no": site, "status": "error", "error": str(error)[:200]}
        manifest.append(row)
        collected += 1
        print("[%d] %s -> %s" % (collected, site, row["status"]), flush=True)
        pd.DataFrame(manifest).to_csv(manifest_path, index=False)
    return pd.DataFrame(manifest)


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

    patch, chosen, status = select_scene_patch(latitude, longitude, scene_window, bands=bands,
                                               patch_size=patch_size)
    if patch is None:
        return {"site_no": site_number, "status": status}

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
            "scene": chosen["product_id"], "tile": chosen["tile"],
            "history_days": int(flow["cfs"].notna().sum())}


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
    manifest: List[Dict] = pd.read_csv(manifest_path, dtype={"site_no": str}).to_dict("records") \
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
    parser.add_argument("--regional", action="store_true",
                        help="Add regional-context sidecars (summer + winter, 25.6 km at 50 m "
                             "by default) for the state's already-collected gauges")
    parser.add_argument("--scene-year", type=int, default=None,
                        help="Scene-window year for --regional (default: last calendar year)")
    parser.add_argument("--regional-size", type=int, default=512)
    parser.add_argument("--regional-pixel-meters", type=float, default=50.0)
    args = parser.parse_args()
    if args.regional:
        manifest = run_regional_collection(args.state, year=args.scene_year, limit=args.limit,
                                           patch_size=args.regional_size,
                                           pixel_meters=args.regional_pixel_meters)
    else:
        manifest = run_state_collection(args.state, history_years=args.history_years,
                                        limit=args.limit)
    print(json.dumps(manifest["status"].value_counts().to_dict(), indent=2))


if __name__ == "__main__":
    main()
