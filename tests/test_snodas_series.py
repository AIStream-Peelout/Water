"""
Offline unit tests for the bulk SNODAS basin-mean series scraper (``snodas_series_scrape``).

Every test here is network-free: the season calendar, output paths and series compilation are pure
logic, the basin averaging runs on synthetic grids, and the ranged tar walk of
:func:`snodas_series_scrape.fetch_swe_members` is exercised against an in-memory tar served by a
stub HTTP session that honours ``Range`` headers.
"""
import gzip
import io
import json
import os
import shutil
import tarfile
import tempfile
import unittest
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from unittest.mock import patch

import numpy as np
import pandas as pd

import snodas_series_scrape
from snodas_series_scrape import (basin_means_from_grid, compile_series, day_json_path,
                                  fetch_swe_members, season_dates)

DATA_MEMBER = "us_ssmv11034tS__T0001TTNATS2023031505HP001.dat.gz"
HEADER_MEMBER = "us_ssmv11034tS__T0001TTNATS2023031505HP001.txt.gz"
OTHER_MEMBER = "us_ssmv11036tS__T0001TTNATS2023031505HP001.dat.gz"


def build_tar_bytes(members: List[Tuple[str, bytes]]) -> bytes:
    """
    Builds an uncompressed tar archive in memory from (member name, payload) pairs.

    :param members: The member names and their raw (already gzipped) payload bytes, in the order
        they should appear in the archive.
    :type members: List[Tuple[str, bytes]]
    :return: The serialized tar bytes.
    :rtype: bytes
    """
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, payload in members:
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
    return buffer.getvalue()


class FakeResponse:
    """A minimal stand-in for :class:`requests.Response` carrying a status code and bytes."""

    def __init__(self, status_code: int, content: bytes) -> None:
        """
        :param status_code: The HTTP status code to report.
        :type status_code: int
        :param content: The response body.
        :type content: bytes
        """
        self.status_code = status_code
        self.content = content

    def raise_for_status(self) -> None:
        """
        Raises when the stubbed status code is an error.

        :return: None
        :rtype: None
        """
        if self.status_code >= 400:
            raise ValueError("HTTP %d" % self.status_code)


class RangeSession:
    """A stub HTTP session that serves ``Range`` requests out of an in-memory byte payload."""

    def __init__(self, payload: bytes, status_code: int = 206) -> None:
        """
        :param payload: The bytes the fake server hosts (a tar archive in these tests).
        :type payload: bytes
        :param status_code: The status code to answer ranged reads with, defaults to 206.
        :type status_code: int, optional
        """
        self.payload = payload
        self.status_code = status_code
        self.ranges: List[Tuple[int, int]] = []

    def get(self, url: str, headers: Optional[Dict[str, str]] = None,
            timeout: Optional[float] = None) -> FakeResponse:
        """
        Answers a ranged GET with the requested slice of the hosted payload.

        :param url: The requested URL (ignored, recorded only through the range list).
        :type url: str
        :param headers: The request headers; must carry a ``Range: bytes=a-b`` entry.
        :type headers: Dict[str, str], optional
        :param timeout: The request timeout (ignored).
        :type timeout: float, optional
        :return: A 206 response holding ``payload[a:b + 1]``.
        :rtype: FakeResponse
        """
        span = (headers or {})["Range"].split("=", 1)[1]
        start, end = (int(part) for part in span.split("-"))
        self.ranges.append((start, end))
        if self.status_code != 206:
            return FakeResponse(self.status_code, b"")
        return FakeResponse(206, self.payload[start:end + 1])


class TestSeasonDates(unittest.TestCase):
    """The Oct 1 - Jul 15 snow-season calendar filter."""

    def test_season_boundaries_are_inclusive(self):
        dates = season_dates(datetime(2023, 9, 29), datetime(2023, 10, 3))
        self.assertEqual(dates, [datetime(2023, 10, 1), datetime(2023, 10, 2),
                                 datetime(2023, 10, 3)])
        july = season_dates(datetime(2024, 7, 13), datetime(2024, 7, 17))
        self.assertEqual(july, [datetime(2024, 7, 13), datetime(2024, 7, 14),
                                datetime(2024, 7, 15)])

    def test_offseason_july16_to_september30_excluded(self):
        self.assertEqual(season_dates(datetime(2024, 7, 16), datetime(2024, 9, 30)), [])

    def test_wraps_across_the_new_year(self):
        dates = season_dates(datetime(2023, 12, 30), datetime(2024, 1, 2))
        self.assertEqual(dates, [datetime(2023, 12, 30), datetime(2023, 12, 31),
                                 datetime(2024, 1, 1), datetime(2024, 1, 2)])

    def test_full_year_counts_only_in_season_days(self):
        dates = season_dates(datetime(2023, 1, 1), datetime(2023, 12, 31))
        # 2023 is not a leap year: Jan 1 - Jul 15 is 196 days, Oct 1 - Dec 31 is 92.
        self.assertEqual(len(dates), 196 + 92)
        self.assertNotIn(datetime(2023, 8, 15), dates)

    def test_empty_when_end_precedes_start(self):
        self.assertEqual(season_dates(datetime(2024, 1, 5), datetime(2024, 1, 1)), [])


class TestDayJsonPath(unittest.TestCase):
    """Naming of the per-day basin-mean JSON files."""

    def test_path_uses_yyyymmdd_under_daily(self):
        with patch.object(snodas_series_scrape, "SERIES_DIR", os.path.join("series", "CO")):
            self.assertEqual(day_json_path(datetime(2023, 3, 15)),
                             os.path.join("series", "CO", "daily", "20230315.json"))

    def test_single_digit_month_and_day_are_zero_padded(self):
        with patch.object(snodas_series_scrape, "SERIES_DIR", "root"):
            self.assertEqual(day_json_path(datetime(2024, 1, 2)),
                             os.path.join("root", "daily", "20240102.json"))


class TestCompileSeries(unittest.TestCase):
    """Folding the daily JSONs into one CSV per basin."""

    def setUp(self):
        """
        Creates a temporary series directory holding synthetic daily JSONs.

        :return: None
        :rtype: None
        """
        self.series_dir = tempfile.mkdtemp()
        self.daily_dir = os.path.join(self.series_dir, "daily")
        os.makedirs(self.daily_dir)
        self.days = {"20231003": {"06752260": 12.5, "06714215": 3.0},
                     "20231002": {"missing": True},
                     "20231001": {"06752260": 10.0, "06714215": 1.0},
                     "20231004": {"06752260": 15.25}}
        for stamp, payload in self.days.items():
            with open(os.path.join(self.daily_dir, stamp + ".json"), "w") as handle:
                json.dump(payload, handle)
        with open(os.path.join(self.daily_dir, "notes.txt"), "w") as handle:
            handle.write("ignored, not a day json")

    def tearDown(self):
        """
        Removes the temporary series directory.

        :return: None
        :rtype: None
        """
        shutil.rmtree(self.series_dir, ignore_errors=True)

    def test_series_are_sorted_and_skip_missing_days(self):
        with patch.object(snodas_series_scrape, "SERIES_DIR", self.series_dir):
            compile_series()
        frame = pd.read_csv(os.path.join(self.series_dir, "06752260_snodas_swe.csv"))
        self.assertEqual(list(frame.columns), ["datetime", "snodas_swe_mm"])
        self.assertEqual(list(frame["datetime"]), ["2023-10-01", "2023-10-03", "2023-10-04"])
        self.assertEqual(list(frame["snodas_swe_mm"]), [10.0, 12.5, 15.25])
        self.assertNotIn("2023-10-02", list(frame["datetime"]))

    def test_each_basin_gets_its_own_file_with_only_its_days(self):
        with patch.object(snodas_series_scrape, "SERIES_DIR", self.series_dir):
            compile_series()
        written = sorted(name for name in os.listdir(self.series_dir) if name.endswith(".csv"))
        self.assertEqual(written, ["06714215_snodas_swe.csv", "06752260_snodas_swe.csv"])
        frame = pd.read_csv(os.path.join(self.series_dir, "06714215_snodas_swe.csv"))
        self.assertEqual(list(frame["datetime"]), ["2023-10-01", "2023-10-03"])
        self.assertEqual(list(frame["snodas_swe_mm"]), [1.0, 3.0])

    def test_all_days_missing_writes_no_files(self):
        for name in os.listdir(self.daily_dir):
            os.remove(os.path.join(self.daily_dir, name))
        with open(os.path.join(self.daily_dir, "20240101.json"), "w") as handle:
            json.dump({"missing": True}, handle)
        with patch.object(snodas_series_scrape, "SERIES_DIR", self.series_dir):
            compile_series()
        self.assertEqual([name for name in os.listdir(self.series_dir) if name.endswith(".csv")],
                         [])


class TestBasinMeansFromGrid(unittest.TestCase):
    """Masked averaging of a parsed SWE grid."""

    def setUp(self):
        """
        Builds a 3x3 synthetic SWE grid with one NaN cell and its flat basin masks.

        :return: None
        :rtype: None
        """
        swe = np.array([[0.0, 10.0, 20.0],
                        [30.0, np.nan, 50.0],
                        [60.0, 70.0, 80.0]])
        self.grid = {"swe_mm": swe}
        self.shape = (3, 3)

    def test_means_over_flat_indices(self):
        masks = {"a": np.array([0, 1, 2]), "b": np.array([6, 7, 8])}
        means = basin_means_from_grid(self.grid, masks, self.shape)
        self.assertAlmostEqual(means["a"], 10.0)
        self.assertAlmostEqual(means["b"], 70.0)

    def test_nan_cells_are_excluded_from_the_mean(self):
        # Cells 3, 4 and 5 are 30, NaN and 50: the mean must ignore the NaN cell entirely.
        means = basin_means_from_grid(self.grid, {"a": np.array([3, 4, 5])}, self.shape)
        self.assertAlmostEqual(means["a"], 40.0)

    def test_all_nan_basin_returns_nan(self):
        means = basin_means_from_grid(self.grid, {"a": np.array([4])}, self.shape)
        self.assertTrue(np.isnan(means["a"]))

    def test_empty_mask_returns_nan(self):
        means = basin_means_from_grid(self.grid, {"a": np.array([], dtype=int)}, self.shape)
        self.assertTrue(np.isnan(means["a"]))

    def test_results_are_plain_floats(self):
        means = basin_means_from_grid(self.grid, {"a": np.array([0, 2])}, self.shape)
        self.assertIsInstance(means["a"], float)
        self.assertNotIsInstance(means["a"], np.ndarray)

    def test_shape_mismatch_raises(self):
        with self.assertRaises(ValueError):
            basin_means_from_grid(self.grid, {"a": np.array([0])}, (3351, 6935))

    def test_empty_mask_dict_returns_empty_dict(self):
        self.assertEqual(basin_means_from_grid(self.grid, {}, self.shape), {})


class TestFetchSweMembers(unittest.TestCase):
    """The ranged tar header walk that pulls only the product 1034 members."""

    def setUp(self):
        """
        Builds an in-memory SNODAS-like tar holding a non-SWE member plus the SWE pair.

        :return: None
        :rtype: None
        """
        self.data_payload = gzip.compress(b"\x00\x01" * 512)
        self.header_payload = gzip.compress(b"Number of rows: 3351\n")
        self.other_payload = gzip.compress(b"snow depth product, not SWE" * 40)
        self.tar_bytes = build_tar_bytes([(OTHER_MEMBER, self.other_payload),
                                          (DATA_MEMBER, self.data_payload),
                                          (HEADER_MEMBER, self.header_payload)])

    def test_returns_the_1034_pair_and_skips_other_products(self):
        session = RangeSession(self.tar_bytes)
        with patch.object(snodas_series_scrape, "_session", return_value=session):
            header_gz, data_gz = fetch_swe_members(datetime(2023, 3, 15))
        self.assertEqual(header_gz, self.header_payload)
        self.assertEqual(data_gz, self.data_payload)
        self.assertEqual(gzip.decompress(data_gz), b"\x00\x01" * 512)
        member_reads = [span for span in session.ranges if span[1] - span[0] + 1 != 512]
        self.assertEqual(len(member_reads), 2)
        self.assertNotIn(len(self.other_payload),
                         [span[1] - span[0] + 1 for span in member_reads])

    def test_header_before_data_still_returns_header_first(self):
        session = RangeSession(build_tar_bytes([(HEADER_MEMBER, self.header_payload),
                                                (DATA_MEMBER, self.data_payload)]))
        with patch.object(snodas_series_scrape, "_session", return_value=session):
            header_gz, data_gz = fetch_swe_members(datetime(2023, 3, 15))
        self.assertEqual(header_gz, self.header_payload)
        self.assertEqual(data_gz, self.data_payload)

    def test_missing_day_returns_none(self):
        class NotFoundSession(RangeSession):
            def get(self, url, headers=None, timeout=None):
                return FakeResponse(404, b"")

        with patch.object(snodas_series_scrape, "_session",
                          return_value=NotFoundSession(self.tar_bytes)):
            self.assertIsNone(fetch_swe_members(datetime(2023, 3, 15)))

    def test_server_ignoring_range_raises(self):
        session = RangeSession(self.tar_bytes, status_code=200)
        with patch.object(snodas_series_scrape, "_session", return_value=session):
            with self.assertRaises(ValueError):
                fetch_swe_members(datetime(2023, 3, 15))

    def test_tar_without_swe_members_raises(self):
        session = RangeSession(build_tar_bytes([(OTHER_MEMBER, self.other_payload)]))
        with patch.object(snodas_series_scrape, "_session", return_value=session):
            with self.assertRaises(ValueError):
                fetch_swe_members(datetime(2023, 3, 15))


if __name__ == "__main__":
    unittest.main()
