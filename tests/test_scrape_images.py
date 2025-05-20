import unittest
import os
from datetime import datetime
from scraping_functions import HydroScraper, BiqQueryConnector
class TestUsgsScraping(unittest.TestCase):
    def setUp(self):
        start_date = datetime(2025, 5, 1)
        end_date = datetime(2025, 5, 18)
        self.test_data_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_data")
        self.scraper = HydroScraper(start_date, end_date, os.path.join(self.test_data_dir, "test_meta.json"))
        unittest.TestLoader.sortTestMethodsUsing = None
    def test_scrape_images(self):
        self.scraper.scrape_images(prefix="overlay/CO_Arkansas_River_near_Nathrop/")
