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

    def test_make_usgs_data(self):
        self.assertEqual(len(self.scraper.usgs_df), 97)

    def test_col_renamer(self):
        pass

    def test_df_label(self):
        pass
