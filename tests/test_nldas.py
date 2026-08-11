from datetime import datetime
import os
import unittest

from nldas_functions import (parse_giovanni_csv, get_nldas_forcing, get_earthdata_token,
                             NLDAS_FORCING_VARIABLES, summarize_missing)

SAMPLE_GIOVANNI_CSV = """Title,NLDAS2 time series
Data id,NLDAS_FORA0125_H_2_0_SWdown
Location,"[40.0, -105.0]"

Timestamp (UTC),Data
2023-06-01T00:00:00,120.5
2023-06-01T01:00:00,45.2
2023-06-01T02:00:00,-9999.0
2023-06-01T03:00:00,0.0
"""


class TestGiovanniParsing(unittest.TestCase):
    """Offline tests for the Giovanni CSV parser and token handling (always run in CI)."""

    def test_parse_giovanni_csv(self):
        df = parse_giovanni_csv(SAMPLE_GIOVANNI_CSV, "shortwave_radiation")
        self.assertEqual(list(df.columns), ["datetime", "shortwave_radiation"])
        self.assertEqual(len(df), 4)
        self.assertEqual(str(df["datetime"].dt.tz), "UTC")
        self.assertAlmostEqual(df["shortwave_radiation"].iloc[0], 120.5)

    def test_fill_values_become_nan(self):
        df = parse_giovanni_csv(SAMPLE_GIOVANNI_CSV, "swdown")
        self.assertTrue(df["swdown"].isna().iloc[2])
        self.assertEqual(summarize_missing(df), {"swdown": 1})

    def test_parse_rejects_html_error_pages(self):
        with self.assertRaises(ValueError):
            parse_giovanni_csv("<html>Service moved</html>", "swdown")

    def test_missing_token_raises_helpful_error(self):
        old_token = os.environ.pop("EARTHDATA_TOKEN", None)
        try:
            with self.assertRaises(RuntimeError):
                get_earthdata_token()
        finally:
            if old_token is not None:
                os.environ["EARTHDATA_TOKEN"] = old_token

    def test_variable_catalog_covers_gr4_needs(self):
        for needed in ["shortwave_radiation", "precipitation", "temperature", "potential_evaporation"]:
            self.assertIn(needed, NLDAS_FORCING_VARIABLES)


@unittest.skipUnless(os.environ.get("EARTHDATA_TOKEN"), "EARTHDATA_TOKEN not set")
class TestGiovanniLive(unittest.TestCase):
    """Live NLDAS-2 fetch through Giovanni; runs only when an Earthdata token is configured."""

    def test_fetch_radiation_and_pet(self):
        df = get_nldas_forcing(40.0, -105.0, datetime(2023, 6, 1), datetime(2023, 6, 1, 12),
                               variables=["shortwave_radiation", "potential_evaporation"])
        self.assertIn("shortwave_radiation", df.columns)
        self.assertIn("potential_evaporation", df.columns)
        self.assertGreaterEqual(len(df), 12)
        self.assertGreater(df["shortwave_radiation"].max(), 100.0)


if __name__ == "__main__":
    unittest.main()
