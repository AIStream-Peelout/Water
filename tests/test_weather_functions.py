
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
        site_number = "663:CO:SNTL"  # triplet
        result = get_snotel_daily_data(start_date, end_date, site_number)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 32)

    def test_format_dt(self):
        date_time_str = "2020-01-01 00:00"
        result = format_dt(date_time_str)
        self.assertEqual(result, datetime(2020, 1, 1, 0, 0))