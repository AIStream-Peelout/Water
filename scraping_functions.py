from datetime import datetime, timedelta
import json
import requests
from typing import Tuple, Dict
import pandas as pd
from usgs_scraping_functions import df_label
from weather_scraping_functions import get_asos_data_from_url, process_asos_csv, pytz


class HydroScraper(object):
    def __init__(self, start_time: datetime, end_time: datetime, meta_data_path: str) -> None:
        with open(meta_data_path, "r") as f:
            self.meta_data = json.load(f)
        self.meta_data["site_number"] = str(self.meta_data["id"])
        if len(self.meta_data["site_number"]) == 7:
            self.meta_data["site_number"] = "0" + self.meta_data["site_number"]
        self.start_time = start_time
        self.end_time = end_time
        self.usgs_df = self.make_usgs_data(self.meta_data["site_number"])
        base_url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station={}&data=tmpf&data=dwpf&data=p01m&data=mslp&data=drct&data=ice_accretion_1hr&year1={}&month1={}&day1={}&year2={}&month2={}&day2={}&tz=Etc%2FUTC&format=onlycomma&latlon=no&missing=M&trace=T&direct=no&report_type=1&report_type=2"
        asos_path = get_asos_data_from_url(self.meta_data["stations"][0]["station_id"], base_url, self.start_time, self.end_time + timedelta(days=1), self.meta_data, self.meta_data)
        self.asos_df = process_asos_csv(asos_path)  # TODO fix this
        print("Scraping completed.")

    def make_usgs_data(self, site_number: str) -> pd.DataFrame:
        """
        Function that scrapes data from gages from a specified start_time THROUGH
        a specified end_time. Returns hourly df of river flow data. For instance:

        ..
        from datetime import datetime
        df = make_usgs_data(datetime(2020, 5, 1), datetime(2021, 5, 1) "01010500")
        df[1:] # would return time stamps of 5/1 in fifteen minute increments (e.g 97)
        len(df[1:]) # 96 The first row is a junk row and real data starts second row (e.g. 96)
        ..

        """
        # //waterservices.usgs.gov/nwis/iv/?format=rdb,1.0&sites={}&startDT={}&endDT={}&parameterCd=00060,00065,00045&siteStatus=all
        base_url = "http://waterservices.usgs.gov/nwis/iv/?format=rdb,1.0&sites={}&startDT={}&endDT={}&parameterCd=00060,00065,00045&siteStatus=all"
        full_url = base_url.format(site_number, self.start_time.strftime("%Y-%m-%d"), self.end_time.strftime("%Y-%m-%d"))
        print("Getting request from USGS")
        print(full_url)
        r = requests.get(full_url)
        with open(site_number + ".txt", "w") as f:
            f.write(r.text)
        print("Request finished")
        response_data = self.process_response_text(site_number + ".txt")
        return self.create_csv(response_data[0], response_data[1], site_number)

    def combine_data(self) -> None:
        tz = pytz.timezone("UTC")
        self.asos_df['hour_updated'] = self.asos_df['hour_updated'].map(lambda x: x.tz_localize("UTC"))
        joined_df = self.asos_df.merge(self.usgs_df, left_on='hour_updated', right_on='datetime', how='inner')
        nan_precip = sum(pd.isnull(joined_df['p01m']))
        nan_flow = sum(pd.isnull(joined_df['cfs']))
        self.joined_df = joined_df
        self.nan_flow = nan_flow
        self.nan_precip = nan_precip
        self.joined_df = joined_df

    @staticmethod
    def create_csv(file_path: str, params_names: dict, site_number: str):
        """
        Function that creates the final version of the CSV file. Called by `make_usgs_data`
        """
        df = pd.read_csv(file_path, sep="\t")
        for key, value in params_names.items():
            df[value] = df[key]
        df.to_csv(site_number + "_flow_data.csv")
        return df


    @staticmethod
    def process_response_text(file_name: str)->Tuple[str, Dict]:
        """Loops through the response text and writes it to TS file. Called by `make_usgs_data`

        :param file_name: _description_
        :type file_name: str
        :return: _description_
        :rtype: Tuple[str, Dict]
        """
        extractive_params = {}
        with open(file_name, "r") as f:
            lines = f.readlines()
            i = 0
            params = False
            while "#" in lines[i]:
                # TODO figure out getting height and discharge code efficently
                the_split_line = lines[i].split()[1:]
                if params:
                    print(the_split_line)
                    if len(the_split_line)<2:
                        params = False
                    else:
                        extractive_params[the_split_line[0]+"_"+the_split_line[1]] = df_label(the_split_line[2])
                if len(the_split_line)>2:
                    if the_split_line[0] == "TS":
                        params = True
                i+=1
            with open(file_name.split(".")[0] + "data.tsv", "w") as t:
                t.write("".join(lines[i:]))
            return file_name.split(".")[0] + "data.tsv", extractive_params


class BiqQueryConnector(object):
    pass
