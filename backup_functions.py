"""
Incremental backup of scraped data directories to Google Cloud Storage.

Project rule: everything we scrape gets periodically mirrored to GCS for redundancy — some sources
(notably the USGS NIMS webcam bucket, a rolling ~15-month archive) cannot be re-scraped later, so the
GCS copy is the durable record. The sync is incremental: files whose blob already exists with the same
size are skipped, so repeated runs only pay for new data.

One-time setup: authenticate with ``gcloud auth application-default login`` (or point
``GOOGLE_APPLICATION_CREDENTIALS`` at a service-account key).

Run manually or from cron, e.g. nightly::

    python backup_functions.py --dir pilot_data --prefix pilot_data
"""
import argparse
import os
from typing import Dict

from google.cloud import storage

DEFAULT_BUCKET = "flow_hydro_2_data"
DEFAULT_PROJECT = "hydro-earthnet-db"


def upload_directory_to_gcs(local_dir: str, bucket_name: str = DEFAULT_BUCKET, prefix: str = "",
                            project: str = DEFAULT_PROJECT, skip_existing: bool = True) -> Dict[str, int]:
    """
    Recursively mirrors a local directory to a GCS bucket, skipping already-uploaded files.

    :param local_dir: The local directory to back up.
    :type local_dir: str
    :param bucket_name: The destination bucket, defaults to "flow_hydro_2_data".
    :type bucket_name: str, optional
    :param prefix: Blob name prefix inside the bucket (e.g. "pilot_data"), defaults to "".
    :type prefix: str, optional
    :param project: The GCP project of the bucket, defaults to "hydro-earthnet-db".
    :type project: str, optional
    :param skip_existing: Skip files whose blob already exists with the same byte size,
        defaults to True.
    :type skip_existing: bool, optional
    :return: A summary dict with "uploaded", "skipped" and "bytes_uploaded" counts.
    :rtype: Dict[str, int]
    """
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    uploaded, skipped, bytes_uploaded = 0, 0, 0
    for root, _, files in os.walk(local_dir):
        for name in files:
            local_path = os.path.join(root, name)
            relative = os.path.relpath(local_path, local_dir)
            blob_name = "/".join(filter(None, [prefix, relative.replace(os.sep, "/")]))
            size = os.path.getsize(local_path)
            if skip_existing:
                existing = bucket.get_blob(blob_name)
                if existing is not None and existing.size == size:
                    skipped += 1
                    continue
            bucket.blob(blob_name).upload_from_filename(local_path)
            uploaded += 1
            bytes_uploaded += size
    return {"uploaded": uploaded, "skipped": skipped, "bytes_uploaded": bytes_uploaded}


def main() -> None:
    """
    CLI entry point for backing up a directory to GCS.

    :return: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Incrementally back up a directory to GCS.")
    parser.add_argument("--dir", required=True, help="Local directory to back up")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="Destination GCS bucket")
    parser.add_argument("--prefix", default=None,
                        help="Blob prefix in the bucket (default: the directory's basename)")
    parser.add_argument("--project", default=DEFAULT_PROJECT, help="GCP project of the bucket")
    parser.add_argument("--force", action="store_true", help="Re-upload even if blobs already exist")
    args = parser.parse_args()
    prefix = args.prefix if args.prefix is not None else os.path.basename(os.path.normpath(args.dir))
    summary = upload_directory_to_gcs(args.dir, bucket_name=args.bucket, prefix=prefix,
                                      project=args.project, skip_existing=not args.force)
    print("Backup complete: %d uploaded (%.1f MB), %d skipped (already in gs://%s/%s)" %
          (summary["uploaded"], summary["bytes_uploaded"] / 1e6, summary["skipped"],
           args.bucket, prefix))


if __name__ == "__main__":
    main()
