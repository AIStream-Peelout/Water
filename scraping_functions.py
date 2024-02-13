from datetime import datetime, timedelta
import json
import requests
from typing import Tuple, Dict
import pandas as pd
from usgs_scraping_functions import df_label, rename_cols
from scrape_text import timezone_map
from weather_scraping_functions import get_asos_data_from_url, process_asos_csv
import pytz
from weather_scraping_functions import get_snotel_data
from google.cloud import bigquery, storage
from redis import Redis
import os


class HydroScraper(object):
    def __init__(self, start_time: datetime, end_time: datetime, meta_data_path: str, asos_bq_table="weather_asos_test") -> None:
        self.r = Redis(
            host='redis-12962.c325.us-east-1-4.ec2.cloud.redislabs.com',
            port=12962,
            db=0,
            username="default",
            password=os.environ["REDIS_PASSWORD"],
            decode_responses=True
        )
        with open(meta_data_path, "r") as f:
            self.meta_data = json.load(f)
        self.meta_data["site_number"] = str(self.meta_data["id"])
        if len(self.meta_data["site_number"]) == 7:
            self.meta_data["site_number"] = "0" + self.meta_data["site_number"]
        self.start_time = start_time
        self.end_time = end_time
        self.usgs_df = rename_cols(self.make_usgs_data(self.meta_data["site_number"]))
        self.final_usgs = self.process_intermediate_csv(self.usgs_df)[0]
        # https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=AIO&data=tmpf&data=dwpf&data=relh&data=feel&data=sknt&data=sped&data=alti&data=p01m&data=vsby&data=gust&data=skyc1&data=peak_wind_gust&data=snowdepth&year1=2024&month1=1&day1=1&year2=2024&month2=1&day2=25&tz=Etc%2FUTC&format=onlycomma&latlon=no&elev=no&missing=M&trace=T&direct=no&report_type=3&report_type=4
        # base_url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station={}&data=tmpf&data=dwpf&data=p01m&data=mslp&data=drct&data=ice_accretion_1hr&year1={}&month1={}&day1={}&year2={}&month2={}&day2={}&tz=Etc%2FUTC&format=onlycomma&latlon=no&missing=M&trace=T&direct=no&report_type=1&report_type=2"
        base_url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station={}&data=tmpf&data=dwpf&data=relh&data=feel&data=sknt&data=sped&data=alti&data=mslp&data=drct&data=ice_accretion_1hr&data=p01m&data=vsby&data=gust&data=skyc1&data=peak_wind_gust&data=snowdepth&year1={}&month1={}&day1={}&year2={}&month2={}&day2={}&tz=Etc%2FUTC&format=onlycomma&latlon=no&elev=no&missing=M&trace=T&direct=no&report_type=3&report_type=4"
        asos_path = get_asos_data_from_url(self.meta_data["stations"][0]["station_id"], base_url, self.start_time, self.end_time + timedelta(days=2), self.meta_data, self.meta_data)
        self.asos_df, self.precip, self.temp = process_asos_csv(asos_path)
        self.asos_df["station_id"] = self.meta_data["stations"][0]["station_id"]
        print("Scraping completed")
        self.bq_connect = BiqQueryConnector()
        res = False
        if self.r.get(self.meta_data["stations"][0]["station_id"] + "_" + str(self.start_time) + "_" + str(self.end_time)) is None:
            res = self.bq_connect.write_to_bq(self.asos_df, asos_bq_table)
        if res:
            print("ASOS data written to BigQuery")
            self.r.set(self.meta_data["stations"][0]["station_id"] + "_" + str(self.start_time) + "_" + str(self.end_time), "True")

    @staticmethod
    def process_intermediate_csv(df: pd.DataFrame) -> (pd.DataFrame, int, int, int):
        """
        Converts local time to UTC time, counts NaN values, gets max/min flows
        """
        # Remove garbage first row
        # TODO check if more rows are garabage
        df = df.iloc[1:]
        time_zone = df["tz_cd"].iloc[0]
        time_zone = timezone_map[time_zone]
        old_timezone = pytz.timezone(time_zone)
        new_timezone = pytz.timezone("UTC")
        # This assumes timezones are consistent throughout the USGS stream (this should be true)
        df["datetime"] = df["datetime"].map(lambda x: old_timezone.localize(datetime.strptime(x, "%Y-%m-%d %H:%M")).astimezone(new_timezone))
        df["cfs"] = pd.to_numeric(df['cfs'], errors='coerce')
        if "height" in df.columns:
            df["height"] = pd.to_numeric(df['height'], errors='coerce')
        if "precip_usgs" in df.columns:
            df["precip_usgs"] = pd.to_numeric(df['precip_usgs'], errors='coerce')
        max_flow = df["cfs"].max()
        min_flow = df["cfs"].min()
        # doesn't do anything with count of nan values?
        count_nan = len(df["cfs"]) - df["cfs"].count()
        return df[df.datetime.dt.minute == 0].reset_index(), max_flow, min_flow, count_nan

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
        if self.asos_df.hour_updated.dt.tz is None:
            self.asos_df['hour_updated'] = self.asos_df['hour_updated'].map(lambda x: x.tz_localize("UTC"))
        joined_df = self.asos_df.merge(self.final_usgs, left_on='hour_updated', right_on='datetime', how='inner')
        nan_precip = sum(pd.isnull(joined_df['p01m']))
        nan_flow = sum(pd.isnull(joined_df['cfs']))
        self.joined_df = joined_df
        self.nan_flow = nan_flow
        self.nan_precip = nan_precip
        self.joined_df = joined_df
        self.joined_df.drop(columns=["site_no"], inplace=True)
        columns_to_drop = [col for col in self.joined_df.columns if col.endswith('_cd')]
        self.joined_df.drop(columns=columns_to_drop, inplace=True)

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
                i += 1
            with open(file_name.split(".")[0] + "data.tsv", "w") as t:
                t.write("".join(lines[i:]))
            return file_name.split(".")[0] + "data.tsv", extractive_params

    def combine_snotel_with_df(self):
        """ to combine the SNOTEL data with the joined ASOS and USGS data.
        """
        self.snotel_df = get_snotel_data(self.start_time, self.end_time, self.meta_data["snotel"]["triplet"])
        self.snotel_df["Date"] = pd.to_datetime(self.snotel_df["Date"], utc=True)
        self.final_df = self.joined_df.merge(self.snotel_df, left_on="hour_updated", right_on="Date", how="left")
        self.final_df["filled_snow"] = self.final_df["Snow Depth (in)"].interpolate(method='nearest').ffill().bfill()

    def combine_sentinel(self, sentinel_df, tile) -> None:
        """ to combine the Sentinel data with the joined ASOS, USGS, and SNOTEL data.
        """
        sentinel_df = sentinel_df[sentinel_df["mgrs_tile"]==tile] 
        sentinel_df = sentinel_df[["sensing_time", "base_url"]]
        sentinel_df["sensing_time"] = pd.to_datetime(sentinel_df["sensing_time"], utc=True, format='mixed').round('60min')
        self.final_df = self.final_df.merge(sentinel_df, left_on="hour_updated", right_on="SENSING_TIME", how="left")

    def write_final_df_to_bq(self, table_name: str):
        self.bq_connect.write_to_bq(self.final_df, table_name)


class BiqQueryConnector(object):
    def __init__(self) -> None:
        self.client = bigquery.Client(project="hydro-earthnet-db")
        self.gcs_client = storage.Client(project="hydro-earthnet-db")

    def write_to_bq(self, df: pd.DataFrame, table_name: str) -> bool:
        table_id = "hydronet." + table_name
        job = self.client.load_table_from_dataframe(df, table_id)
        print(job.result())
        return True

    def upload_file_to_gcs(self, df, site_no, bucket_name="flow_hydro_2_data", file_type="joined_df"):
        csv_file = df.to_csv()
        bucket = self.gcs_client.get_bucket(bucket_name)
        gcs_path = file_type
        # Define the blob path
        blob = bucket.blob(os.path.join(gcs_path, site_no + ".csv"))

        # Upload the CSV data to the blob
        blob.upload_from_string(csv_file, content_type='text/csv')


class SCANScraper(object):
    """_summary_

    :param object: _description_
    :type object: _type_
    """
    def __init__(self) -> None:
        pass
