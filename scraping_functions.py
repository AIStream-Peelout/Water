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
import logging
import boto3
from botocore import UNSIGNED
from usgs_scraping_functions import Config
import time
import re

class HydroScraper(object):
    def __init__(self, start_time: datetime, end_time: datetime, meta_data_path: str, asos_bq_table="weather_asos_test", use_redis=False, use_bq=False) -> None:
        """
        Class to scrape USGS and ASOS data and save the data to BigQuery.
        :param start_time: The start time of the data to scrape.
        :type start_time: datetime
        :param end_time: The end time of the data to scrape.
        :type end_time: datetime
        :param meta_data_path: The path to the metadata file.
        :type meta_data_path: str
        :param asos_bq_table: The name of the BigQuery table to save the ASOS data to.
        :type asos_bq_table: str
        :param use_redis: Whether to use Redis to store the start and end times of the data. This effectively disables
        the scraper from running twice in a row.
        :type use_redis: bool
        """
        self.use_redis = use_redis
        if use_redis:
            self.r = Redis(
                host=os.environ["REDIS_HOST"],
                port=12962,
                db=0,
                username="default",
                password=os.environ["REDIS_PASSWORD"],
                decode_responses=True
            )
        with open(meta_data_path, "r") as f:
            self.meta_data = json.load(f)
        self.meta_data["site_number"] = str(self.meta_data["id"])
        # Due to initial scrapping errors that casted the gage id as integer there some gages that do not have
        if len(self.meta_data["site_number"]) == 7:
            self.meta_data["site_number"] = "0" + self.meta_data["site_number"]
        self.start_time = start_time
        self.end_time = end_time
        self.usgs_df = rename_cols(self.make_usgs_data(self.meta_data["site_number"]))
        self.final_usgs = self.process_intermediate_csv(self.usgs_df)[0]
        # https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=AIO&data=tmpf&data=dwpf&data=relh&data=feel&data=sknt&data=sped&data=alti&data=p01m&data=vsby&data=gust&data=skyc1&data=peak_wind_gust&data=snowdepth&year1=2024&month1=1&day1=1&year2=2024&month2=1&day2=25&tz=Etc%2FUTC&format=onlycomma&latlon=no&elev=no&missing=M&trace=T&direct=no&report_type=3&report_type=4
        # base_url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station={}&data=tmpf&data=dwpf&data=p01m&data=mslp&data=drct&data=ice_accretion_1hr&year1={}&month1={}&day1={}&year2={}&month2={}&day2={}&tz=Etc%2FUTC&format=onlycomma&latlon=no&missing=M&trace=T&direct=no&report_type=1&report_type=2"
        base_url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station={}&data=tmpf&data=dwpf&data=relh&data=feel&data=sknt&data=sped&data=alti&data=mslp&data=drct&data=ice_accretion_1hr&data=p01m&data=vsby&data=gust&data=skyc1&data=peak_wind_gust&data=snowdepth&year1={}&month1={}&day1={}&year2={}&month2={}&day2={}&tz=Etc%2FUTC&format=onlycomma&latlon=no&elev=no&missing=M&trace=T&direct=no&report_type=3&report_type=4"
        i = 0
        # Sometimes the ASOS data is not available for the first station, so we need to loop until we get data.
        run_loop = True
        while run_loop:
            asos_path = get_asos_data_from_url(self.meta_data["stations"][i]["station_id"], base_url, self.start_time, self.end_time + timedelta(days=2), self.meta_data, self.meta_data)
            self.asos_df, self.precip, self.temp = process_asos_csv(asos_path)
            if len(self.asos_df) > 0:
                run_loop = False
            self.asos_df["station_id"] = self.meta_data["stations"][i]["station_id"]
            i+=1
        print("Scraping completed")
        if use_bq:
            self.bq_connect = BiqQueryConnector()
        res = False
        if self.use_redis:
            if self.r.get(self.meta_data["stations"][0]["station_id"] + "_" + str(self.start_time) + "_" + str(self.end_time)) is None:
                res = self.bq_connect.write_to_bq(self.asos_df, asos_bq_table)
            if res:
                print("ASOS data written to BigQuery")
                self.r.set(self.meta_data["stations"][0]["station_id"] + "_" + str(self.start_time) + "_" + str(self.end_time), "True")

    @staticmethod
    def process_intermediate_csv(df: pd.DataFrame) -> (pd.DataFrame, int, int, int):
        """
        Converts local time to UTC time, counts NaN values, gets max/min flows.
        """
        # Remove garbage first row
        # TODO check if more rows are garbage
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

        ...
        from datetime import datetime
        df = make_usgs_data(datetime(2020, 5, 1), datetime(2021, 5, 1) "01010500")
        df[1:] # would return time stamps of 5/1 in fifteen minute increments (e.g. 97)
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
        """Loops through the response text and writes it to the TS file. Called by the`make_usgs_data function.

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
                    if len(the_split_line) < 2:
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

    def combine_snotel_with_df(self, snotel_present = True):
        """ Function to combine the SNOTEL data with the joined ASOS and USGS data.
        """
        if snotel_present is False:
            self.final_df= self.joined_df
            return
        self.snotel_df = get_snotel_data(self.start_time, self.end_time, self.meta_data["snotel"]["triplet"])
        self.snotel_df["Date"] = pd.to_datetime(self.snotel_df["Date"], utc=True)
        self.final_df = self.joined_df.merge(self.snotel_df, left_on="hour_updated", right_on="Date", how="left")
        self.final_df["filled_snow"] = self.final_df["Snow Depth (in)"].interpolate(method='nearest').ffill().bfill()

    def combine_sentinel(self, sentinel_df: pd.DataFrame, tile: str) -> None:
        """Function to combine the Sentinel data with the joined ASOS, USGS, and SNOTEL data.
        :param sentinel_df: The large up-to-date Sentinel data to combine. How to get this data remains a question.
        :type sentinel_df: pd.DataFrame
        :param tile: The tile number to filter the Sentinel data by.
        :type tile: str
        """
        sentinel_df = sentinel_df[sentinel_df["mgrs_tile"]==tile]
        sentinel_df = sentinel_df[["sensing_time", "base_url"]]
        sentinel_df["sensing_time"] = pd.to_datetime(sentinel_df["sensing_time"], utc=True, format='mixed').round('60min')
        self.final_df = self.final_df.merge(sentinel_df, left_on="hour_updated", right_on="sensing_time", how="left")

    def write_final_df_to_bq(self, table_name: str):
        self.bq_connect.write_to_bq(self.final_df, table_name)

    def scrape_images(self, output_dir: str = "river_images",
                                    bucket_name: str = "usgs-nims-images",
                                    prefix: str = None,
                                    date_filter: datetime = None) -> dict:
        """
        Lists and downloads all webcam images from a public S3 bucket that match the criteria.

        Args:
            output_dir: Directory to save images
            bucket_name: Name of the S3 bucket
            prefix: Prefix to filter objects in the bucket (e.g., "overlay/CO_Arkansas_River_near_Nathrop/")
                    If None, will try to construct using metadata
            date_filter: If provided, only download images from this date

        Returns:
            Dictionary mapping datetime to local image paths
        """
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        os.makedirs(output_dir, exist_ok=True)

        # Create anonymous S3 client (for public buckets)
        s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

        # If prefix not provided, try to create one from metadata
        if prefix is None:
            if "state" in self.meta_data and "station_nm" in self.meta_data:
                state = self.meta_data.get("state", "")
                river_name = self.meta_data.get("station_nm", "").replace(" ", "_")
                camera_id = f"{state}_{river_name}"
                prefix = f"overlay/{camera_id}/"
            else:
                logger.warning("Could not construct prefix from metadata. Using empty prefix.")
                prefix = ""

        logger.info(f"Listing objects in bucket: {bucket_name} with prefix: {prefix}")

        # Initialize result dictionary
        image_paths = {}

        # For pagination
        continuation_token = None

        # Define regex pattern to extract datetime from filenames
        # Pattern like: CO_Arkansas_River_near_Nathrop___2025-05-04T16-30-53Z.jpg
        datetime_pattern = re.compile(r'___(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z)\.jpg$')

        # Track files for debugging
        total_files = 0
        matching_files = 0

        while True:
            # List objects in the bucket
            if continuation_token:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    ContinuationToken=continuation_token
                )
            else:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix
                )

            # Process each object
            if 'Contents' in response:
                for obj in response['Contents']:
                    total_files += 1
                    key = obj['Key']

                    # Extract datetime from filename
                    match = datetime_pattern.search(key)
                    if match:
                        timestamp_str = match.group(1)
                        try:
                            # Parse the datetime
                            img_datetime = datetime.strptime(timestamp_str, "%Y-%m-%dT%H-%M-%SZ")

                            # Apply date filter if provided
                            if date_filter is not None:
                                if img_datetime.date() != date_filter.date():
                                    continue
                            # Apply range filter if no specific date provided
                            elif not (self.start_time <= img_datetime <= self.end_time):
                                continue

                            matching_files += 1

                            # Create local filename
                            site_id = self.meta_data.get("site_number", "unknown")
                            local_filename = os.path.join(
                                output_dir,
                                f"{site_id}_{img_datetime.strftime('%Y%m%d_%H%M%S')}.jpg"
                            )

                            # Download the image
                            logger.info(f"Downloading {key} to {local_filename}")
                            s3_client.download_file(bucket_name, key, local_filename)

                            # Store mapping of datetime to image path
                            image_paths[img_datetime] = local_filename

                            # Add delay to avoid overwhelming the server
                            time.sleep(0.1)
                        except ValueError as e:
                            logger.warning(f"Could not parse datetime from {timestamp_str}: {e}")

            # Check if there are more results
            if not response.get('IsTruncated'):
                break

            continuation_token = response.get('NextContinuationToken')

        logger.info(f"Found {total_files} total files, {matching_files} matching date criteria")
        logger.info(f"Successfully downloaded {len(image_paths)} images")

        return image_paths

    def find_s3_camera_prefixes(self, bucket_name: str = "usgs-nims-images") -> list:
        """
        Searches the S3 bucket to find all possible camera prefixes.
        Useful when you don't know the exact prefix pattern.

        Args:
            bucket_name: Name of the S3 bucket

        Returns:
            List of camera prefixes found in the bucket
        """
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        # Create anonymous S3 client
        s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

        # Possible prefixes to check
        prefixes_to_check = ["overlay/", "images/", ""]

        # Find actual station name from metadata
        state = self.meta_data.get("state", "")
        river_name = self.meta_data.get("station_nm", "")
        station_number = self.meta_data.get("site_number", "")

        # Generate possible patterns to look for
        search_patterns = []

        if state and river_name:
            # Standard format: ST_River_Name
            search_patterns.append(f"{state}_{river_name.replace(' ', '_')}")

            # Try with different separators
            search_patterns.append(f"{state}-{river_name.replace(' ', '-')}")
            search_patterns.append(f"{state}{river_name.replace(' ', '')}")

            # Try with site number
            if station_number:
                search_patterns.append(f"{state}_{station_number}")
                search_patterns.append(f"{station_number}")

        found_prefixes = []

        for base_prefix in prefixes_to_check:
            logger.info(f"Checking base prefix: {base_prefix}")

            # List the common prefixes at this level
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=base_prefix,
                Delimiter='/'
            )

            # Check common prefixes
            if 'CommonPrefixes' in response:
                for prefix_obj in response['CommonPrefixes']:
                    prefix = prefix_obj['Prefix']
                    logger.info(f"Found prefix: {prefix}")

                    # Check if this prefix might match our station
                    for pattern in search_patterns:
                        if pattern.lower() in prefix.lower():
                            found_prefixes.append(prefix)
                            logger.info(f"✓ Matched pattern '{pattern}': {prefix}")

            # Check direct listing for any matching files
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']

                    for pattern in search_patterns:
                        if pattern.lower() in key.lower():
                            # Extract the prefix part
                            parts = key.split('/')
                            if len(parts) > 1:
                                derived_prefix = '/'.join(parts[:-1]) + '/'
                                if derived_prefix not in found_prefixes:
                                    found_prefixes.append(derived_prefix)
                                    logger.info(f"✓ Derived prefix from key '{key}': {derived_prefix}")
                            break

        logger.info(f"Found {len(found_prefixes)} possible camera prefixes")
        return found_prefixes

    def download_all_station_images(self, output_dir: str = "river_images"):
        """
        Comprehensive method to find and download all images for the current station.
        First attempts to discover the correct S3 prefixes, then downloads all matching images.

        Args:
            output_dir: Directory to save images

        Returns:
            Dictionary mapping datetime to local image paths
        """
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        # First, try to find all possible prefixes
        possible_prefixes = self.find_s3_camera_prefixes()

        # If we found any prefixes, use them to download images
        if possible_prefixes:
            all_images = {}

            for prefix in possible_prefixes:
                logger.info(f"Downloading images using prefix: {prefix}")
                images = self.list_and_download_s3_images(
                    output_dir=output_dir,
                    prefix=prefix
                )

                # Merge images into all_images
                all_images.update(images)

            return all_images
        else:
            # If no prefixes found, try with a constructed prefix
            state = self.meta_data.get("state", "")
            river_name = self.meta_data.get("station_nm", "").replace(" ", "_")
            camera_id = f"{state}_{river_name}"
            prefix = f"overlay/{camera_id}/"

            logger.info(f"No prefixes discovered. Trying constructed prefix: {prefix}")
            return self.list_and_download_s3_images(
                output_dir=output_dir,
                prefix=prefix
            )

    def add_image_paths_to_df(self):
        """
        Adds image paths to the final dataframe by matching timestamps.
        """
        # Get image paths
        image_paths = self.scrape_images()

        # Convert image paths dict to dataframe
        image_df = pd.DataFrame.from_dict(image_paths, orient='index', columns=['image_path'])
        image_df.index.name = 'datetime'
        image_df = image_df.reset_index()

        # Round timestamps to hour to match with final_df
        image_df['hour_updated'] = image_df['datetime'].dt.round('H')

        # Group by hour and aggregate paths into lists
        image_df = image_df.groupby('hour_updated')['image_path'].agg(lambda x: list(x)).reset_index()

        # Merge with final_df
        if hasattr(self, 'final_df'):
            self.final_df = self.final_df.merge(image_df, on='hour_updated', how='left')
        elif hasattr(self, 'joined_df'):
            self.joined_df = self.joined_df.merge(image_df, on='hour_updated', how='left')
        else:
            raise AttributeError("No dataframe found to merge image paths with")
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
    """Class to scrape SCAN data from the USDA website and save files to CSVs and BigQuery.
    :param object: _description_
    :type object: _type_
    """
    def __init__(self) -> None:
        self.base_url = "https://www.wcc.nrcs.usda.gov/nwcc/site?sitenum={}&state={}&county={}&agency=NRCS"
        self.scan_df = self.get_scan_data()
        self.bq_connect = BiqQueryConnector()