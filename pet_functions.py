"""
Potential evapotranspiration (PET) estimation from temperature.

Implements the Hargreaves-Samani (1985) method, which needs only daily min/max temperature and latitude.
It serves as the radiation-free fallback for gauges/periods where NLDAS-2 potential evaporation (see
``nldas_functions``) is unavailable. All equations follow FAO-56 (Allen et al., 1998) conventions.
"""
import math
from typing import Optional

import numpy as np
import pandas as pd

SOLAR_CONSTANT_MJ = 0.0820  # MJ m^-2 min^-1
MJ_PER_M2_TO_MM = 0.408  # latent heat conversion, mm of water per MJ/m^2


def extraterrestrial_radiation(latitude: float, day_of_year: np.ndarray) -> np.ndarray:
    """
    Computes daily extraterrestrial radiation Ra (FAO-56 eq. 21) in mm/day water equivalent.

    :param latitude: The latitude in decimal degrees (positive north).
    :type latitude: float
    :param day_of_year: The day(s) of year (1-366), scalar or array.
    :type day_of_year: np.ndarray
    :return: Extraterrestrial radiation in mm/day water equivalent, same shape as day_of_year.
    :rtype: np.ndarray
    """
    doy = np.asarray(day_of_year, dtype=float)
    phi = math.radians(latitude)
    dr = 1.0 + 0.033 * np.cos(2.0 * math.pi / 365.0 * doy)
    delta = 0.409 * np.sin(2.0 * math.pi / 365.0 * doy - 1.39)
    sunset_angle = np.arccos(np.clip(-math.tan(phi) * np.tan(delta), -1.0, 1.0))
    ra_mj = (24.0 * 60.0 / math.pi) * SOLAR_CONSTANT_MJ * dr * (
        sunset_angle * math.sin(phi) * np.sin(delta) +
        math.cos(phi) * np.cos(delta) * np.sin(sunset_angle)
    )
    return ra_mj * MJ_PER_M2_TO_MM


def hargreaves_pet(tmin_c: np.ndarray, tmax_c: np.ndarray, latitude: float, day_of_year: np.ndarray,
                   tmean_c: Optional[np.ndarray] = None) -> np.ndarray:
    """
    Computes daily PET with the Hargreaves-Samani equation in mm/day.

    :param tmin_c: Daily minimum temperature(s) in Celsius.
    :type tmin_c: np.ndarray
    :param tmax_c: Daily maximum temperature(s) in Celsius.
    :type tmax_c: np.ndarray
    :param latitude: The latitude in decimal degrees.
    :type latitude: float
    :param day_of_year: The day(s) of year matching the temperature values.
    :type day_of_year: np.ndarray
    :param tmean_c: Daily mean temperature(s) in Celsius, defaults to None which uses (tmin + tmax) / 2.
    :type tmean_c: np.ndarray, optional
    :return: PET in mm/day, clipped at zero, same shape as the inputs.
    :rtype: np.ndarray
    """
    tmin = np.asarray(tmin_c, dtype=float)
    tmax = np.asarray(tmax_c, dtype=float)
    tmean = (tmin + tmax) / 2.0 if tmean_c is None else np.asarray(tmean_c, dtype=float)
    ra = extraterrestrial_radiation(latitude, day_of_year)
    temperature_range = np.clip(tmax - tmin, 0.0, None)
    pet = 0.0023 * ra * np.sqrt(temperature_range) * (tmean + 17.8)
    return np.clip(pet, 0.0, None)


def daylight_weights(latitude: float, day_of_year: int) -> np.ndarray:
    """
    Computes 24 hourly weights (summing to 1) that distribute daily PET over daylight as a half-sine.

    :param latitude: The latitude in decimal degrees.
    :type latitude: float
    :param day_of_year: The day of year.
    :type day_of_year: int
    :return: An array of 24 non-negative weights summing to 1.
    :rtype: np.ndarray
    """
    phi = math.radians(latitude)
    delta = 0.409 * math.sin(2.0 * math.pi / 365.0 * day_of_year - 1.39)
    sunset_angle = math.acos(max(-1.0, min(1.0, -math.tan(phi) * math.tan(delta))))
    daylight_hours = 24.0 / math.pi * sunset_angle
    sunrise = 12.0 - daylight_hours / 2.0
    hours = np.arange(24.0) + 0.5
    weights = np.sin(math.pi * (hours - sunrise) / daylight_hours)
    weights = np.clip(weights, 0.0, None)
    if weights.sum() == 0.0:
        return np.full(24, 1.0 / 24.0)
    return weights / weights.sum()


def add_hargreaves_pet(df: pd.DataFrame, latitude: float, temp_col: str = "tmpf",
                       datetime_col: str = "hour_updated", temp_unit: str = "F",
                       distribution: str = "daylight") -> pd.DataFrame:
    """
    Adds an hourly ``pet_mm_hr`` column to an hourly dataframe (e.g. the joined ASOS/USGS frame).

    Daily PET is computed with Hargreaves-Samani from each day's min/max temperature, then distributed
    over the day's hours either uniformly or as a daylight half-sine.

    :param df: An hourly dataframe containing a temperature and a datetime column.
    :type df: pd.DataFrame
    :param latitude: The latitude of the site in decimal degrees.
    :type latitude: float
    :param temp_col: The temperature column name, defaults to "tmpf" (the ASOS column).
    :type temp_col: str, optional
    :param datetime_col: The datetime column name, defaults to "hour_updated".
    :type datetime_col: str, optional
    :param temp_unit: "F" or "C", defaults to "F".
    :type temp_unit: str, optional
    :param distribution: How to spread daily PET across hours, "daylight" or "uniform",
        defaults to "daylight".
    :type distribution: str, optional
    :return: The dataframe with an added "pet_mm_hr" column.
    :rtype: pd.DataFrame
    """
    if distribution not in ("daylight", "uniform"):
        raise ValueError("distribution must be 'daylight' or 'uniform' but got " + distribution)
    df = df.copy()
    temps = pd.to_numeric(df[temp_col], errors="coerce")
    if temp_unit == "F":
        temps = (temps - 32.0) * 5.0 / 9.0
    timestamps = pd.to_datetime(df[datetime_col])
    dates = timestamps.dt.date
    df["pet_mm_hr"] = 0.0
    for date_value, day_index in df.groupby(dates).groups.items():
        day_temps = temps.loc[day_index]
        if day_temps.isna().all():
            df.loc[day_index, "pet_mm_hr"] = float("nan")
            continue
        doy = pd.Timestamp(date_value).dayofyear
        daily_pet = float(hargreaves_pet(day_temps.min(), day_temps.max(), latitude, doy,
                                         tmean_c=day_temps.mean()))
        hours = timestamps.loc[day_index].dt.hour.to_numpy()
        if distribution == "daylight":
            weights = daylight_weights(latitude, doy)[hours]
        else:
            weights = np.full(len(hours), 1.0 / 24.0)
        df.loc[day_index, "pet_mm_hr"] = daily_pet * weights
    return df
