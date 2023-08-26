# create unittests for the functions in usgs_scraping_functions.py

from datetime import datetime
from usgs_scraping_functions import make_usgs_data, process_response_text, df_label, create_csv, column_renamer, rename_cols, create_csv
import unittest


class TestUsgsScraping(unittest.TestCase):
    def __setUp__(self):
        pass

    def test_make_usgs_data(self):
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2020, 1, 1)
        site_number = "01646500"
        result = make_usgs_data(start_date, end_date, site_number)
        self.assertEqual(len(result), 97)

    def test_col_renamer(self):
        pass

    def test_df_label(self):
        pass
