# create unittests for the functions in usgs_scraping_functions.py

from datetime import datetime
from weather_scraping_functions import process_asos_csv, format_dt, get_snotel_daily_data, combine_asos_snotel
import unittest
import pandas as pd


class TestUsgsScraping(unittest.TestCase):
    def __setUp__(self):
        pass

    def test_get_snotel_daily_data(self):
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 2, 1)
        site_number = "663:CO:SNTL"
        result = get_snotel_daily_data(start_date, end_date, site_number)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 32)
