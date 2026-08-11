"""
State-level fleet scraping of USGS gauges with a persistent progress registry.

Enumerates a state's active streamflow gauges (instantaneous discharge available), then runs the
resumable :func:`long_term_scrape.run_long_term_scrape` for each, recording per-gauge status in
``registry.json`` under the state's output directory. The registry is the source of truth for
search/marking: rerunning the command skips completed gauges, retries failed ones, and each gauge's
chunk files make even a mid-gauge interruption resumable. The registry is mirrored to GCS after every
gauge so progress is visible remotely.

Registry statuses: "completed" (scrape finished; chunk_failures lists any windows that errored),
"failed" (the gauge errored before finishing — the error is recorded; rerun retries it).

Examples::

    python state_scrape.py --state CO                  # scrape (or resume) all of Colorado
    python state_scrape.py --state CO --report         # print progress, no scraping
    python state_scrape.py --state CO --limit 5        # first five pending gauges only
"""
import argparse
import json
import os
import time
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
import requests

from backup_functions import upload_file
from build_pilot_dataset import load_dotenv
from long_term_scrape import run_long_term_scrape

STATE_SITES_URL = ("https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd={}&siteType=ST"
                   "&siteStatus=active&hasDataTypeCd=uv&parameterCd=00060")


def list_state_gauges(state_abbrev: str) -> pd.DataFrame:
    """
    Lists a state's active stream gauges that report instantaneous discharge.

    :param state_abbrev: The two-letter state abbreviation, e.g. "CO".
    :type state_abbrev: str
    :return: A dataframe with site_no, station_nm, dec_lat_va, dec_long_va and huc_cd columns.
    :rtype: pd.DataFrame
    """
    response = requests.get(STATE_SITES_URL.format(state_abbrev), timeout=120)
    response.raise_for_status()
    lines = [line for line in response.text.splitlines() if not line.startswith("#")]
    header = lines[0].split("\t")
    rows = [line.split("\t") for line in lines[2:] if line.startswith("USGS")]
    df = pd.DataFrame(rows, columns=header)
    return df[["site_no", "station_nm", "dec_lat_va", "dec_long_va", "huc_cd"]]


def load_registry(registry_path: str) -> Dict:
    """
    Loads the per-gauge progress registry, returning an empty one when absent.

    :param registry_path: Path to registry.json.
    :type registry_path: str
    :return: The registry dict keyed by site number.
    :rtype: Dict
    """
    if os.path.exists(registry_path):
        with open(registry_path) as f:
            return json.load(f)
    return {}


def save_registry(registry: Dict, registry_path: str, gcs_prefix: Optional[str] = None) -> None:
    """
    Writes the registry locally and mirrors it to GCS.

    :param registry: The registry dict.
    :type registry: Dict
    :param registry_path: Path to registry.json.
    :type registry_path: str
    :param gcs_prefix: The claude_data/ prefix to mirror to, defaults to None which skips the mirror.
    :type gcs_prefix: str, optional
    :return: None
    :rtype: None
    """
    with open(registry_path, "w") as f:
        json.dump(registry, f, indent=2)
    if gcs_prefix is not None:
        upload_file(registry_path, gcs_prefix)


def registry_report(registry: Dict) -> Dict[str, int]:
    """
    Summarizes registry statuses.

    :param registry: The registry dict.
    :type registry: Dict
    :return: Counts per status.
    :rtype: Dict[str, int]
    """
    counts: Dict[str, int] = {}
    for entry in registry.values():
        counts[entry["status"]] = counts.get(entry["status"], 0) + 1
    return counts


def run_state_scrape(state_abbrev: str, output_root: str = os.path.join("pilot_data", "scrapes"),
                     limit: Optional[int] = None, include_nldas: bool = True,
                     gages2_zip_path: Optional[str] = os.path.join("pilot_data", "gages2.zip"),
                     backup: bool = True, retry_failed: bool = True) -> Dict:
    """
    Scrapes (or resumes scraping) every enumerated gauge of a state.

    :param state_abbrev: The two-letter state abbreviation.
    :type state_abbrev: str
    :param output_root: Root directory for state scrape outputs, defaults to pilot_data/scrapes.
    :type output_root: str, optional
    :param limit: Stop after this many gauges are attempted this run, defaults to None (all).
    :type limit: int, optional
    :param include_nldas: Whether to fetch NLDAS-2 forcing per gauge, defaults to True.
    :type include_nldas: bool, optional
    :param gages2_zip_path: GAGES-II archive path for static attributes, defaults to
        pilot_data/gages2.zip.
    :type gages2_zip_path: str, optional
    :param backup: Whether to mirror gauge outputs and the registry to GCS, defaults to True.
    :type backup: bool, optional
    :param retry_failed: Whether to retry gauges previously marked failed, defaults to True.
    :type retry_failed: bool, optional
    :return: The status counts after the run.
    :rtype: Dict
    """
    state_dir = os.path.join(output_root, state_abbrev)
    os.makedirs(state_dir, exist_ok=True)
    registry_path = os.path.join(state_dir, "registry.json")
    gcs_prefix = os.path.normpath(state_dir).replace(os.sep, "/") if backup else None
    gauges = list_state_gauges(state_abbrev)
    gauges.to_csv(os.path.join(state_dir, "gauges.csv"), index=False)
    registry = load_registry(registry_path)
    print("State", state_abbrev, "has", len(gauges), "gauges; registry:", registry_report(registry))

    attempted = 0
    for _, gauge in gauges.iterrows():
        site = gauge["site_no"]
        entry = registry.get(site)
        if entry is not None and entry["status"] == "completed":
            continue
        if entry is not None and entry["status"] == "failed" and not retry_failed:
            continue
        if limit is not None and attempted >= limit:
            break
        attempted += 1
        print("[%s %d/%d] scraping %s (%s)" % (state_abbrev, attempted,
                                               limit or len(gauges), site, gauge["station_nm"]))
        try:
            summary = run_long_term_scrape(site, output_dir=os.path.join(state_dir, site),
                                           include_nldas=include_nldas,
                                           gages2_zip_path=gages2_zip_path, backup=backup)
            registry[site] = {"status": "completed", "station_nm": gauge["station_nm"],
                              "rows": summary["combined_rows"], "start": summary["start"],
                              "chunk_failures": len(summary["chunk_failures"]),
                              "data_availability": summary["data_availability"],
                              "finished_at": datetime.now().isoformat(timespec="seconds")}
        except Exception as error:  # noqa: BLE001 - one bad gauge must not stop the fleet
            registry[site] = {"status": "failed", "station_nm": gauge["station_nm"],
                              "error": str(error)[:300],
                              "finished_at": datetime.now().isoformat(timespec="seconds")}
            print("  FAILED:", str(error)[:200])
            # Back off after a failure: a 503 usually means an NWIS outage window, and racing
            # through the gauge list during one just converts the whole list to failures.
            time.sleep(120)
        save_registry(registry, registry_path, gcs_prefix)
    report = registry_report(registry)
    print("State", state_abbrev, "registry now:", report)
    return report


def main() -> None:
    """
    CLI entry point for state-level fleet scraping.

    :return: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Scrape every active uv-discharge gauge of a state.")
    parser.add_argument("--state", required=True, help="Two-letter state abbreviation, e.g. CO")
    parser.add_argument("--limit", type=int, default=None, help="Max gauges to attempt this run")
    parser.add_argument("--no-nldas", action="store_true", help="Skip NLDAS-2 forcing")
    parser.add_argument("--no-backup", action="store_true", help="Skip GCS mirroring")
    parser.add_argument("--no-retry-failed", action="store_true", help="Do not retry failed gauges")
    parser.add_argument("--report", action="store_true", help="Print registry status and exit")
    args = parser.parse_args()
    load_dotenv()
    if args.report:
        registry = load_registry(os.path.join("pilot_data", "scrapes", args.state, "registry.json"))
        print(json.dumps({"counts": registry_report(registry),
                          "failed": {site: entry["error"] for site, entry in registry.items()
                                     if entry["status"] == "failed"}}, indent=2))
        return
    include_nldas = not args.no_nldas and bool(os.environ.get("EARTHDATA_TOKEN"))
    if not args.no_nldas and not include_nldas:
        print("WARNING: EARTHDATA_TOKEN not set; scraping without NLDAS-2 forcing.")
    run_state_scrape(args.state, limit=args.limit, include_nldas=include_nldas,
                     backup=not args.no_backup, retry_failed=not args.no_retry_failed)


if __name__ == "__main__":
    main()
