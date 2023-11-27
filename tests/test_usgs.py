# create unittests for the functions in usgs_scraping_functions.py

from datetime import datetime
from scraping_functions import HydroScraper
import unittest
import os


class TestUsgsScraping(unittest.TestCase):
    def setUp(self):
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2020, 1, 1)
        self.test_data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_data")
        self.scraper = HydroScraper(start_date, end_date, os.path.join(self.test_data_dir, "test_meta.json"))

    def test_asos_data(self):
        self.assertEqual(len(self.scraper.asos_df), 24)
        self.assertIn("p01m", self.scraper.asos_df.columns)

    def test_make_usgs_data(self):
        self.assertEqual(len(self.scraper.usgs_df), 97)
        self.assertGreater(len(self.scraper.final_usgs), 17)

    def test_combine_data(self):
        self.scraper.combine_data()
        self.assertEqual(len(self.scraper.joined_df), 24)
        self.assertEqual(self.scraper.nan_flow, 0)
