# create unittests for the functions in usgs_scraping_functions.py
from datetime import datetime
from weather_scraping_functions import get_asos_data_from_url, process_asos_csv, format_dt
import unittest


class TestWeatherScraping(unittest.TestCase):
    def __setUp__(self):
        pass

    def test_get_asos_data(self):
        dat = get_asos_data_from_url("KJFK", "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station={}&data=tmpf&data=dwpf&data=p01m&data=ice_accretion_1hr&data=mslp&data=drct&year1={}&month1={}&day1={}&year2={}&month2={}&day2={}&tz=Etc%2FUTC&format=onlycomma&latlon=no&missing=M&trace=T&direct=no&report_type=1&report_type=2", datetime(2020, 1, 1), datetime(2020, 1, 2))
        self.assertEqual(len(dat), 25)