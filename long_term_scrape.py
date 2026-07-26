"""
Resumable long-term scrape of a single gauge from its period-of-record start to the present.

Splits the period into calendar chunks (default 12 months), fetches each chunk with
:func:`catchment_dataset.fetch_hourly_chunk`, and writes one CSV per chunk under ``chunks/``. A chunk
whose file already exists is skipped, so an interrupted multi-hour scrape resumes where it left off.
Failed chunks are recorded and reported but do not stop the run (delete the bad chunk file, if any,
and re-run to retry them). At the end all chunks are concatenated into ``<site>_hourly_full.csv`` and
the output directory is incrementally backed up to GCS per the project backup rule.

The default start date is the gauge's instantaneous ("uv") streamflow begin date from the NWIS series
catalog — the earliest date for which sub-daily flow exists (usually much later than the daily record).

Example::

    python long_term_scrape.py --site 06752260
"""
import argparse
import json
import os
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd

from backup_functions import upload_directory_to_gcs
from build_pilot_dataset import load_dotenv
from catchment_dataset import discover_catchment, fetch_hourly_chunk, get_data_availability
from usgs_scraping_functions import get_period_of_record


def chunk_bounds(start_time: datetime, end_time: datetime,
                 chunk_months: int = 12) -> List[Tuple[datetime, datetime]]:
    """
    Splits a period into consecutive calendar-aligned chunks.

    Each chunk starts on the first day of a month (except possibly the first chunk) and chunks abut
    exactly: the next chunk starts where the previous ended.

    :param start_time: The start of the full period.
    :type start_time: datetime
    :param end_time: The end of the full period.
    :type end_time: datetime
    :param chunk_months: The chunk length in months, defaults to 12.
    :type chunk_months: int, optional
    :return: A list of (chunk_start, chunk_end) tuples covering the period.
    :rtype: List[Tuple[datetime, datetime]]
    """
    bounds = []
    current = start_time
    while current < end_time:
        month_index = (current.year * 12 + (current.month - 1)) + chunk_months
        next_boundary = datetime(month_index // 12, month_index % 12 + 1, 1)
        chunk_end = min(next_boundary, end_time)
        bounds.append((current, chunk_end))
        current = chunk_end
    return bounds


def run_long_term_scrape(site_number: str, start_time: Optional[datetime] = None,
                         end_time: Optional[datetime] = None, output_dir: Optional[str] = None,
                         chunk_months: int = 12, include_nldas: bool = True,
                         gages2_zip_path: Optional[str] = None, backup: bool = True,
                         max_chunks: Optional[int] = None) -> dict:
    """
    Runs (or resumes) the full scrape for one gauge and returns a summary dict.

    :param site_number: The USGS gauge site number.
    :type site_number: str
    :param start_time: The scrape start, defaults to None which uses the uv streamflow begin date.
    :type start_time: datetime, optional
    :param end_time: The scrape end, defaults to None which uses today.
    :type end_time: datetime, optional
    :param output_dir: The output directory, defaults to None which uses pilot_data/<site>_full.
    :type output_dir: str, optional
    :param chunk_months: Months per chunk, defaults to 12.
    :type chunk_months: int, optional
    :param include_nldas: Whether to fetch NLDAS-2 forcing (needs EARTHDATA_TOKEN), defaults to True.
    :type include_nldas: bool, optional
    :param gages2_zip_path: Optional GAGES-II archive path for static attributes, defaults to None.
    :type gages2_zip_path: str, optional
    :param backup: Whether to back the output directory up to GCS at the end, defaults to True.
    :type backup: bool, optional
    :param max_chunks: Stop after this many newly fetched chunks (for tests/pilots), defaults to None.
    :type max_chunks: int, optional
    :return: A summary dict with chunk counts, failures and the combined row count.
    :rtype: dict
    """
    if start_time is None:
        catalog = get_period_of_record(site_number)
        flow_series = catalog.get("uv_00060")
        if flow_series is None:
            raise ValueError("Gauge " + site_number + " has no instantaneous (uv) streamflow record; "
                             "series present: " + str(sorted(catalog)))
        start_time = datetime.strptime(flow_series["begin_date"], "%Y-%m-%d")
    if end_time is None:
        end_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    output_dir = output_dir or os.path.join("pilot_data", site_number + "_full")
    chunks_dir = os.path.join(output_dir, "chunks")
    os.makedirs(chunks_dir, exist_ok=True)

    discovery = discover_catchment(site_number, gages2_zip_path=gages2_zip_path)
    # Record when each source begins so training can pick a fully-aligned start per river.
    discovery["static"]["data_availability"] = get_data_availability(site_number, discovery=discovery)
    with open(os.path.join(output_dir, site_number + "_static.json"), "w") as f:
        json.dump(discovery["static"], f, default=str)
    if discovery["basin_geometry"] is not None:
        with open(os.path.join(output_dir, site_number + "_basin.geojson"), "w") as f:
            json.dump({"type": "Feature", "geometry": discovery["basin_geometry"],
                       "properties": {"site_no": site_number}}, f)

    fetched, skipped, failures = 0, 0, []
    for chunk_start, chunk_end in chunk_bounds(start_time, end_time, chunk_months):
        chunk_path = os.path.join(chunks_dir, site_number + "_" + chunk_start.strftime("%Y%m%d") + ".csv")
        if os.path.exists(chunk_path):
            skipped += 1
            continue
        print("Fetching chunk", chunk_start.date(), "->", chunk_end.date())
        try:
            chunk = fetch_hourly_chunk(site_number, chunk_start, chunk_end, discovery,
                                       include_nldas=include_nldas)
        except Exception as error:  # noqa: BLE001 - a multi-hour scrape must survive flaky sources
            failures.append({"chunk": chunk_start.strftime("%Y-%m-%d"), "error": str(error)[:300]})
            print("  FAILED:", str(error)[:200])
            continue
        chunk.to_csv(chunk_path, index=False)
        fetched += 1
        if max_chunks is not None and fetched >= max_chunks:
            break

    chunk_files = sorted(os.path.join(chunks_dir, name) for name in os.listdir(chunks_dir)
                         if name.endswith(".csv"))
    frames = [pd.read_csv(path) for path in chunk_files]
    frames = [frame for frame in frames if len(frame) > 0]
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not combined.empty:
        combined = combined.drop_duplicates(subset="datetime").sort_values("datetime")
        combined.to_csv(os.path.join(output_dir, site_number + "_hourly_full.csv"), index=False)

    summary = {"site": site_number, "start": str(start_time.date()), "end": str(end_time.date()),
               "chunks_fetched": fetched, "chunks_skipped_existing": skipped,
               "chunk_failures": failures, "combined_rows": int(len(combined)),
               "data_availability": discovery["static"]["data_availability"]}
    with open(os.path.join(output_dir, site_number + "_scrape_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    if backup:
        # Mirror the local path under claude_data/ (e.g. pilot_data/06752260_full) so GCS and local
        # layouts stay identical.
        backup_summary = upload_directory_to_gcs(output_dir,
                                                 prefix=os.path.normpath(output_dir).replace(os.sep, "/"))
        summary["backup"] = backup_summary
    return summary


def main() -> None:
    """
    CLI entry point for the long-term scrape.

    :return: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Scrape a gauge from its uv begin date to present.")
    parser.add_argument("--site", required=True, help="USGS gauge site number")
    parser.add_argument("--start", default=None, help="Override start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="Override end date YYYY-MM-DD")
    parser.add_argument("--output-dir", default=None, help="Output directory")
    parser.add_argument("--chunk-months", type=int, default=12, help="Months per chunk")
    parser.add_argument("--gages2-zip", default=os.path.join("pilot_data", "gages2.zip"))
    parser.add_argument("--no-nldas", action="store_true", help="Skip NLDAS-2 forcing")
    parser.add_argument("--no-backup", action="store_true", help="Skip the GCS backup at the end")
    args = parser.parse_args()
    load_dotenv()
    include_nldas = not args.no_nldas and bool(os.environ.get("EARTHDATA_TOKEN"))
    if not args.no_nldas and not include_nldas:
        print("WARNING: EARTHDATA_TOKEN not set; skipping NLDAS-2 forcing.")
    summary = run_long_term_scrape(
        args.site,
        start_time=datetime.strptime(args.start, "%Y-%m-%d") if args.start else None,
        end_time=datetime.strptime(args.end, "%Y-%m-%d") if args.end else None,
        output_dir=args.output_dir, chunk_months=args.chunk_months, include_nldas=include_nldas,
        gages2_zip_path=args.gages2_zip, backup=not args.no_backup)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
