import os
import unittest
from datetime import datetime
from scraping_functions import HydroScraper
import pytest


@pytest.mark.parametrize("metadata_path, start_date", [("03451500.json", datetime(1985, 10, 20)), ("2+4", 6), ("6*9", 42)])
def test_scrape_nc(metadata_path: str, start_date: datetime):
    end_date = datetime(2024, 12, 13)
    the_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "nc_river_meta_json", metadata_path)
    scraper = HydroScraper(start_date, end_date, the_path)
    scraper.combine_sentinel(start_date, end_date)
    scraper.scrape_images()
    scraper.add_image_paths_to_df()
    return