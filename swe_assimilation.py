"""
Basin snow-state estimation from the SNOTEL network (lightweight data assimilation).

Samples every SNOTEL site inside a basin's bounding box on a given date, fits SWE against elevation
(snow line + linear accumulation above it), and evaluates the fit at a set of equal-area elevation
bands derived from the basin's GAGES-II elevation statistics. The result initializes the elevation-
banded snow states of the hybrid model with *observed* antecedent conditions — resolving the
point-to-basin scaling that a single site cannot (e.g. Cache la Poudre, June 5 2024: sites below
10,000 ft all read 0 mm while sites above hold 96-417 mm).
"""
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from awdb_functions import get_awdb_element_data, get_awdb_stations

FEET_TO_M = 0.3048
INCHES_TO_MM = 25.4


def sample_basin_snotel(basin_bbox: List[float], date: datetime,
                        state_code: Optional[str] = None) -> pd.DataFrame:
    """
    Reads SWE at every SNOTEL site inside a basin bounding box on one date.

    :param basin_bbox: The basin bounding box as (min_lon, min_lat, max_lon, max_lat).
    :type basin_bbox: List[float]
    :param date: The date to sample.
    :type date: datetime
    :param state_code: Optional two-letter state filter to reduce the station listing,
        defaults to None.
    :type state_code: str, optional
    :return: A dataframe with name, elevation_m and swe_mm per in-basin site (NaN SWE dropped).
    :rtype: pd.DataFrame
    """
    min_lon, min_lat, max_lon, max_lat = basin_bbox
    stations = get_awdb_stations(network="SNTL", state_code=state_code)
    inside = stations[(stations.longitude >= min_lon) & (stations.longitude <= max_lon) &
                      (stations.latitude >= min_lat) & (stations.latitude <= max_lat)]
    records = []
    for _, station in inside.iterrows():
        swe = get_awdb_element_data(station["stationTriplet"], ["WTEQ"], date, date,
                                    duration="DAILY")
        if len(swe) == 0 or "WTEQ" not in swe.columns or swe["WTEQ"].isna().all():
            continue
        records.append({"name": station["name"], "triplet": station["stationTriplet"],
                        "elevation_m": float(station["elevation"]) * FEET_TO_M,
                        "swe_mm": float(swe["WTEQ"].iloc[0]) * INCHES_TO_MM})
    return pd.DataFrame(records)


def equal_area_bands(elev_mean_m: float, elev_std_m: float, n_bands: int = 5) -> Dict:
    """
    Builds equal-area elevation bands from a normal approximation of the basin hypsometry.

    Band elevations are the quantile midpoints of N(elev_mean, elev_std) — with GAGES-II providing
    only summary statistics, the normal approximation is the standard low-data hypsometric proxy.

    :param elev_mean_m: Mean basin elevation in meters (GAGES-II ELEV_MEAN_M_BASIN).
    :type elev_mean_m: float
    :param elev_std_m: Basin elevation standard deviation in meters (GAGES-II ELEV_STD_M_BASIN).
    :type elev_std_m: float
    :param n_bands: The number of equal-area bands, defaults to 5.
    :type n_bands: int, optional
    :return: A dict with "elevations_m" (band midpoint elevations, low to high) and
        "area_fractions" (equal fractions summing to 1).
    :rtype: Dict
    """
    from scipy.stats import norm
    quantiles = (np.arange(n_bands) + 0.5) / n_bands
    elevations = norm.ppf(quantiles, loc=elev_mean_m, scale=max(elev_std_m, 1.0))
    return {"elevations_m": elevations.tolist(),
            "area_fractions": [1.0 / n_bands] * n_bands}


def fit_swe_elevation_profile(samples: pd.DataFrame, band_elevations_m: List[float]) -> List[float]:
    """
    Fits a snow-line profile to SNOTEL samples and evaluates it at band elevations.

    The profile is ``SWE(z) = max(0, slope * (z - snow_line))``: zero below the snow line and
    linearly increasing above it. The snow line is placed between the highest dry site and the
    lowest snow-bearing site; the slope is a least-squares fit through the snow-bearing sites.

    :param samples: The per-site dataframe from :func:`sample_basin_snotel`.
    :type samples: pd.DataFrame
    :param band_elevations_m: Elevations at which to evaluate the profile.
    :type band_elevations_m: List[float]
    :return: Estimated SWE in mm per band (same order as band_elevations_m).
    :rtype: List[float]
    """
    if samples.empty:
        return [0.0] * len(band_elevations_m)
    snowy = samples[samples["swe_mm"] > 1.0]
    dry = samples[samples["swe_mm"] <= 1.0]
    if snowy.empty:
        return [0.0] * len(band_elevations_m)
    if dry.empty:
        snow_line = float(snowy["elevation_m"].min()) - 200.0
    else:
        highest_dry = float(dry["elevation_m"].max())
        lowest_snowy = float(snowy["elevation_m"].min())
        snow_line = (highest_dry + min(lowest_snowy, highest_dry + 400.0)) / 2.0 \
            if lowest_snowy > highest_dry else highest_dry
    above = (snowy["elevation_m"] - snow_line).to_numpy()
    above = np.clip(above, 1.0, None)
    slope = float((above * snowy["swe_mm"].to_numpy()).sum() / (above * above).sum())
    slope = max(slope, 0.0)
    return [max(0.0, slope * (z - snow_line)) for z in band_elevations_m]


def estimate_band_swe(static: Dict, date: datetime, n_bands: int = 5,
                      state_code: Optional[str] = None) -> Dict:
    """
    End-to-end basin snow-state estimate: SNOTEL sampling + hypsometric bands + profile fit.

    :param static: The gauge's static attribute dict (needs basin_bbox, gages2_ELEV_MEAN_M_BASIN and
        gages2_ELEV_STD_M_BASIN).
    :type static: Dict
    :param date: The date to estimate for (e.g. the simulation window start).
    :type date: datetime
    :param n_bands: The number of elevation bands, defaults to 5.
    :type n_bands: int, optional
    :param state_code: Optional state filter for the station listing, defaults to None.
    :type state_code: str, optional
    :return: A dict with "band_elevations_m", "area_fractions", "band_swe_mm", "basin_mean_swe_mm"
        and the raw "samples" dataframe.
    :rtype: Dict
    """
    bands = equal_area_bands(float(static["gages2_ELEV_MEAN_M_BASIN"]),
                             float(static["gages2_ELEV_STD_M_BASIN"]), n_bands=n_bands)
    samples = sample_basin_snotel(static["basin_bbox"], date, state_code=state_code)
    band_swe = fit_swe_elevation_profile(samples, bands["elevations_m"])
    basin_mean = float(np.dot(band_swe, bands["area_fractions"]))
    return {"band_elevations_m": bands["elevations_m"], "area_fractions": bands["area_fractions"],
            "band_swe_mm": band_swe, "basin_mean_swe_mm": basin_mean, "samples": samples}
