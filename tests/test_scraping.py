import unittest
from scraping_functions import EHydroScraper
from datetime import datetime
import os

class TestUsgClasssScraping(unittest.TestCase):
    def __setUp__(self):
        self.scaper = EHydroScraper(datetime(2020, 5, 1), datetime(2021, 5, 1), os.path.join("meta_data.json") 