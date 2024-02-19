# create unittests for the functions in usgs_scraping_functions.py
from datetime import datetime
from scraping_functions import HydroScraper, BiqQueryConnector
from weather_scraping_functions import get_snotel_data
import unittest
import os
import pandas as pd


class TestUsgsScraping(unittest.TestCase):
    def setUp(self):
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2020, 1, 1)
        self.test_data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_data")
        self.scraper = HydroScraper(start_date, end_date, os.path.join(self.test_data_dir, "test_meta.json"))
        self.western_scraper = HydroScraper(datetime(2021, 12, 1), datetime(2022, 3, 1), os.path.join(self.test_data_dir, "west_meta.json"))
        unittest.TestLoader.sortTestMethodsUsing = None

    def test_asos_data(self):
        self.assertEqual(len(self.scraper.asos_df), 47)  # 47 because we scraped additional day due to time zone issues.
        self.assertIn("p01m", self.scraper.asos_df.columns)

    def test_make_usgs_data(self):
        self.assertEqual(len(self.scraper.usgs_df), 97)
        self.assertGreater(len(self.scraper.final_usgs), 17)
        self.assertEqual(len(self.scraper.final_usgs), 24)

    def test_combine_data(self):
        bq_connect = BiqQueryConnector()
        bq_connect.write_to_bq(self.scraper.asos_df, "weather_asos_test")
        self.scraper.combine_data()
        self.assertEqual(len(self.scraper.joined_df), 24)
        self.assertEqual(self.scraper.nan_precip, 0)

    def test_get_snotel_data(self):
        snotel_df = get_snotel_data(self.scraper.start_time, self.scraper.end_time, "427:MT:SNTL")
        self.assertIsInstance(snotel_df, pd.DataFrame)

    def test_snotel_west(self):
        self.western_scraper.combine_data()
        # TODO figure out hour discrpency while scraping..
        # self.assertEqual(len(self.western_scraper.usgs_df) / 4, 2184). most 1 hour short
        # self.assertEqual(len(self.western_scraper.asos_df), 2184)
        self.western_scraper.combine_snotel_with_df()
        # self.assertEqual(len(self.western_scraper.joined_df), 2174)
        self.assertIn("filled_snow", self.western_scraper.final_df.columns)
        sentinel_csv = pd.read_csv(os.path.join(self.test_data_dir, "exam_west1.csv"))
        self.western_scraper.combine_sentinel(sentinel_csv, "13TDE")
        self.western_scraper.final_df.to_csv("test.csv")
        self.assertIn("sensing_time", self.western_scraper.final_df.columns)
        self.assertIn("base_url", self.western_scraper.final_df.columns)
