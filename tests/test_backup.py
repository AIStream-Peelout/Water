import os
import tempfile
import unittest

import google.auth
from google.auth.exceptions import DefaultCredentialsError

from backup_functions import upload_directory_to_gcs, DEFAULT_BUCKET, DEFAULT_PROJECT


def has_gcp_credentials() -> bool:
    """
    Checks whether Application Default Credentials are available.

    :return: True when GCP credentials can be resolved.
    :rtype: bool
    """
    try:
        google.auth.default()
        return True
    except DefaultCredentialsError:
        return False


@unittest.skipUnless(has_gcp_credentials(), "No GCP application default credentials")
class TestGcsBackup(unittest.TestCase):
    """Live round-trip test: upload a tiny directory, verify incremental skip, then clean up."""

    def test_incremental_upload(self):
        # Project rule: never delete from GCP, so this test uses a fixed prefix with a fixed payload
        # and tolerates the blob already existing from a previous run instead of cleaning up.
        from google.cloud import storage
        with tempfile.TemporaryDirectory() as local_dir:
            sub = os.path.join(local_dir, "sub")
            os.makedirs(sub)
            with open(os.path.join(sub, "sample.txt"), "w") as f:
                f.write("backup test payload")
            first = upload_directory_to_gcs(local_dir, prefix="test_backup_ci")
            self.assertEqual(first["uploaded"] + first["skipped"], 1)
            second = upload_directory_to_gcs(local_dir, prefix="test_backup_ci")
            self.assertEqual(second["uploaded"], 0)
            self.assertEqual(second["skipped"], 1)
        client = storage.Client(project=DEFAULT_PROJECT)
        blob = client.bucket(DEFAULT_BUCKET).blob("claude_data/test_backup_ci/sub/sample.txt")
        self.assertTrue(blob.exists())


if __name__ == "__main__":
    unittest.main()
