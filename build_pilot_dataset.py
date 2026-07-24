"""
CLI to build a complete pilot dataset for a single USGS gauge over a time range.

Produces everything the catchment foundation model consumes, for one river, so the data can be inspected
end to end before scaling to many gauges:

* ``<site>_static.json`` — NWIS attributes + nearest SCAN station + GAGES-II basin characteristics.
* ``<site>_basin.geojson`` — upstream basin polygon (NLDI).
* ``<site>_hourly.csv`` — hourly UTC frame: USGS flow/height, SCAN soil moisture at all depths, and
  (with an Earthdata token) NLDAS-2 forcing incl. radiation and PET.
* ``patches/`` — Sentinel-2 patches (npy) plus ``sentinel_manifest.csv`` with cloud cover per scene.

Example (the Cache la Poudre pilot)::

    python build_pilot_dataset.py --site 06752260 --start 2024-06-01 --end 2024-06-30
"""
import argparse
import json
import os
from datetime import datetime

from camera_functions import download_camera_images, find_camera_prefix
from catchment_dataset import build_catchment_bundle
from sentinel_functions import build_patch_time_series
from weather_scraping_functions import FIPS_TO_STATE


def load_dotenv(path: str = ".env") -> None:
    """
    Loads KEY=VALUE lines from a .env file into the environment (without overriding existing values).

    :param path: The .env file path, defaults to ".env".
    :type path: str, optional
    :return: None
    :rtype: None
    """
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def main() -> None:
    """
    Parses arguments, builds the bundle and Sentinel-2 patches, and prints a data-quality summary.

    :return: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Build a pilot catchment dataset for one gauge.")
    parser.add_argument("--site", default="06752260", help="USGS gauge site number")
    parser.add_argument("--start", default="2024-06-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="2024-06-30", help="End date YYYY-MM-DD")
    parser.add_argument("--output-dir", default=None, help="Output directory (default pilot_data/<site>)")
    parser.add_argument("--gages2-zip", default=os.path.join("pilot_data", "gages2.zip"),
                        help="Cache path for the GAGES-II archive")
    parser.add_argument("--patch-size", type=int, default=128, help="Sentinel patch size in 10m pixels")
    parser.add_argument("--max-cloud", type=float, default=80.0,
                        help="Skip Sentinel scenes cloudier than this percent")
    parser.add_argument("--no-nldas", action="store_true", help="Skip NLDAS-2 forcing")
    parser.add_argument("--no-sentinel", action="store_true", help="Skip Sentinel-2 patches")
    parser.add_argument("--no-camera", action="store_true", help="Skip NIMS webcam images")
    parser.add_argument("--camera-interval-minutes", type=float, default=60.0,
                        help="Minimum spacing between archived webcam images")
    args = parser.parse_args()

    load_dotenv()
    start_time = datetime.strptime(args.start, "%Y-%m-%d")
    end_time = datetime.strptime(args.end, "%Y-%m-%d")
    output_dir = args.output_dir or os.path.join("pilot_data", args.site)
    include_nldas = not args.no_nldas and bool(os.environ.get("EARTHDATA_TOKEN"))
    if not args.no_nldas and not include_nldas:
        print("WARNING: EARTHDATA_TOKEN not set; skipping NLDAS-2 forcing.")

    bundle = build_catchment_bundle(args.site, start_time, end_time, output_dir=output_dir,
                                    include_nldas=include_nldas, gages2_zip_path=args.gages2_zip)
    hourly = bundle["hourly"]
    static = bundle["static"]

    manifest = None
    if not args.no_sentinel:
        manifest = build_patch_time_series(static["dec_lat_va"], static["dec_long_va"], start_time,
                                           end_time, output_dir=os.path.join(output_dir, "patches"),
                                           patch_size=args.patch_size,
                                           max_cloud_percent=args.max_cloud)

    camera_manifest = None
    camera_prefix = None
    if not args.no_camera:
        camera_prefix = find_camera_prefix(FIPS_TO_STATE[static["state_cd"]], static["station_nm"])
        if camera_prefix is None:
            print("NOTE: no NIMS webcam found for this gauge. Reminder: the NIMS bucket only retains "
                  "~15 months of images, so camera archives must be collected continuously.")
        else:
            camera_manifest = download_camera_images(camera_prefix, os.path.join(output_dir, "camera"),
                                                     start_time, end_time,
                                                     min_interval_minutes=args.camera_interval_minutes)

    summary = {
        "site": args.site,
        "period": [args.start, args.end],
        "hourly_rows": len(hourly),
        "hourly_columns": list(hourly.columns),
        "missing_per_column": {c: int(hourly[c].isna().sum()) for c in hourly.columns},
        "static_attribute_count": len(static),
        "sentinel_scenes_kept": None if manifest is None else len(manifest),
        "camera_prefix": camera_prefix,
        "camera_images": None if camera_manifest is None else len(camera_manifest),
    }
    with open(os.path.join(output_dir, args.site + "_summary.json"), "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
