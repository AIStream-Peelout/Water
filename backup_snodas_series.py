"""
Mirrors the SNODAS basin-mean SWE series to Google Cloud Storage.

Thin runner over :func:`backup_functions.upload_directory_to_gcs` for the output of
``snodas_series_scrape`` (``pilot_data/snodas_series/<state>/``: one ``<site>_snodas_swe.csv`` per
basin, the ``basin_masks.npz`` archive and the resume-safe ``daily/`` JSONs). The local relative
path is mirrored under the ``claude_data/`` namespace, i.e.
``gs://flow_hydro_2_data/claude_data/pilot_data/snodas_series/CO/...``.

The sync is incremental and add-only: files whose blob already exists with the same byte size are
skipped, and nothing is ever deleted from the bucket, so this is safe to re-run while a scrape is
still filling in ``daily/``.

Usage::

    python backup_snodas_series.py
    python backup_snodas_series.py --state CO
"""
import argparse
import os
from typing import Dict

from backup_functions import BACKUP_ROOT_PREFIX, DEFAULT_BUCKET, DEFAULT_PROJECT, \
    upload_directory_to_gcs

SERIES_ROOT = os.path.join("pilot_data", "snodas_series")


def backup_snodas_series(state: str = "CO", bucket_name: str = DEFAULT_BUCKET,
                         project: str = DEFAULT_PROJECT,
                         skip_existing: bool = True) -> Dict[str, int]:
    """
    Uploads one state's SNODAS series directory to GCS, mirroring its local relative path.

    :param state: The state subdirectory of ``pilot_data/snodas_series`` to mirror, defaults to
        "CO".
    :type state: str, optional
    :param bucket_name: The destination bucket, defaults to "flow_hydro_2_data".
    :type bucket_name: str, optional
    :param project: The GCP project of the bucket, defaults to "hydro-earthnet-db".
    :type project: str, optional
    :param skip_existing: Skip files whose blob already exists with the same byte size, defaults to
        True.
    :type skip_existing: bool, optional
    :return: The summary dict from :func:`backup_functions.upload_directory_to_gcs`.
    :rtype: Dict[str, int]
    """
    local_dir = os.path.join(SERIES_ROOT, state)
    if not os.path.isdir(local_dir):
        raise FileNotFoundError("No SNODAS series directory at " + local_dir)
    return upload_directory_to_gcs(local_dir, bucket_name=bucket_name,
                                   prefix=local_dir.replace(os.sep, "/"), project=project,
                                   skip_existing=skip_existing)


def main() -> None:
    """
    CLI entry point for mirroring the SNODAS series directory to GCS.

    :return: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Incrementally back up SNODAS series to GCS.")
    parser.add_argument("--state", default="CO", help="State subdirectory to mirror")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="Destination GCS bucket")
    parser.add_argument("--project", default=DEFAULT_PROJECT, help="GCP project of the bucket")
    parser.add_argument("--force", action="store_true",
                        help="Re-upload even if blobs already exist")
    args = parser.parse_args()
    summary = backup_snodas_series(state=args.state, bucket_name=args.bucket, project=args.project,
                                   skip_existing=not args.force)
    print("SNODAS series backup complete: %d uploaded (%.1f MB), %d skipped -> gs://%s/%s/%s/%s" %
          (summary["uploaded"], summary["bytes_uploaded"] / 1e6, summary["skipped"], args.bucket,
           BACKUP_ROOT_PREFIX, SERIES_ROOT.replace(os.sep, "/"), args.state))


if __name__ == "__main__":
    main()
