
import pandas as pd
from datetime import datetime
from typing import Tuple, Dict
import requests
import boto3
from botocore import UNSIGNED
from botocore.config import Config


def make_usgs_data(start_date: datetime, end_date: datetime, site_number: str):
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
    full_url = base_url.format(site_number, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    print("Getting request from USGS")
    print(full_url)
    r = requests.get(full_url)
    with open(site_number + ".txt", "w") as f:
        f.write(r.text)
    response_data = process_response_text(site_number + ".txt")
    create_csv(response_data[0], response_data[1], site_number)
    return pd.read_csv(site_number + "_flow_data.csv")


def column_renamer(x):
    """_summary_

    :param x: The column names of the dataframe as a string
    :type x: _str
    :return: _description_
    :rtype: _type_
    """
    code_converter_1 = {"00060": "cfs", "00065": "height", "00045": "precip_usgs"}
    split_x = x.split("_")
    if len(split_x) > 1:
        if split_x[1] in code_converter_1 and "cd" not in x:
            return code_converter_1[split_x[1]]
    return x


def rename_cols(df) -> pd.DataFrame:
    """_summary_

    :param df: _description_
    :type df: _type_
    :return: _description_
    :rtype: pd.DataFrame
    """
    df.columns = df.columns.map(column_renamer)
    return df


def process_response_text(file_name: str)->Tuple[str, Dict]:
    """_summary_

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


def df_label(usgs_text: str) -> str:
    """_summary_

    :param usgs_text: _description
    :type usgs_text: str
    :return: _description_
    :rtype: str
    """
    usgs_text = usgs_text.replace(",", "")
    if usgs_text == "Discharge":
        return "cfs"
    elif usgs_text == "Gage":
        return "height"
    else:
        return usgs_text


def create_csv(file_path: str, params_names: dict, site_number: str): 
    """
    Function that creates the final version of the CSV file
    Assigns
    """
    print(params_names)
    df = pd.read_csv(file_path, sep="\t")
    for key, value in params_names.items():
        df[value] = df[key]
    df.to_csv(site_number + "_flow_data.csv")
