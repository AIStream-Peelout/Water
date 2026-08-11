"""
Coarse DEM sampling and per-elevation-band statistics for gridded snow products.

Elevation comes from the AWS Open Data terrain tiles (the former Mapzen "terrarium" PNG tiles at
https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png, anonymous access) — at zoom 9
a pixel is roughly 230 m at mid-latitudes, comfortably finer than the 1-4 km snow grids it is paired
with. The module also provides the polygon masking shared by the SNODAS and UA SWE scrapers: GeoJSON
basin polygons (from ``usgs_scraping_functions.get_basin_boundary``) are rasterized onto a grid's cell
centers with ``matplotlib.path`` so no GDAL/shapely dependency is needed.
"""
import io
import math
from typing import Dict, List, Optional

import numpy as np
import requests

TERRARIUM_TILE_URL = "https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"
TILE_SIZE = 256


def normalize_geometry(geometry: Dict) -> Dict:
    """
    Unwraps a GeoJSON Feature or FeatureCollection down to a bare geometry dict.

    :param geometry: A GeoJSON geometry, Feature or single-feature FeatureCollection.
    :type geometry: Dict
    :return: The geometry dict (type + coordinates).
    :rtype: Dict
    """
    if geometry.get("type") == "FeatureCollection":
        features = geometry.get("features", [])
        if len(features) != 1:
            raise ValueError("Expected exactly one feature, got " + str(len(features)))
        geometry = features[0]
    if geometry.get("type") == "Feature":
        geometry = geometry["geometry"]
    if geometry.get("type") not in ("Polygon", "MultiPolygon"):
        raise ValueError("Unsupported GeoJSON geometry type: " + str(geometry.get("type")))
    return geometry


def polygon_mask(lats: np.ndarray, lons: np.ndarray, geometry: Dict) -> np.ndarray:
    """
    Rasterizes a GeoJSON polygon onto a regular lat/lon grid's cell centers.

    Interior rings (holes) are subtracted; MultiPolygons are the union of their parts.

    :param lats: The 1-D array of grid cell-center latitudes.
    :type lats: np.ndarray
    :param lons: The 1-D array of grid cell-center longitudes.
    :type lons: np.ndarray
    :param geometry: A GeoJSON Polygon/MultiPolygon geometry (Features are unwrapped).
    :type geometry: Dict
    :return: A boolean array of shape (len(lats), len(lons)), True inside the polygon.
    :rtype: np.ndarray
    """
    from matplotlib.path import Path
    geometry = normalize_geometry(geometry)
    polygons = geometry["coordinates"] if geometry["type"] == "MultiPolygon" \
        else [geometry["coordinates"]]
    lon_grid, lat_grid = np.meshgrid(np.asarray(lons, dtype=float), np.asarray(lats, dtype=float))
    points = np.column_stack([lon_grid.ravel(), lat_grid.ravel()])
    mask = np.zeros(len(points), dtype=bool)
    for rings in polygons:
        inside = Path(np.asarray(rings[0], dtype=float)[:, :2]).contains_points(points)
        for hole in rings[1:]:
            inside &= ~Path(np.asarray(hole, dtype=float)[:, :2]).contains_points(points)
        mask |= inside
    return mask.reshape(len(lats), len(lons))


def _tile_indices(latitude: float, longitude: float, zoom: int) -> tuple:
    """
    Converts a lat/lon to slippy-map tile indices at a zoom level.

    :param latitude: The latitude in decimal degrees.
    :type latitude: float
    :param longitude: The longitude in decimal degrees.
    :type longitude: float
    :param zoom: The tile zoom level.
    :type zoom: int
    :return: The (x, y) tile indices.
    :rtype: tuple
    """
    n = 2 ** zoom
    x = int((longitude + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(math.radians(latitude))) / math.pi) / 2.0 * n)
    return min(max(x, 0), n - 1), min(max(y, 0), n - 1)


def sample_dem_grid(bbox: List[float], zoom: int = 9) -> Dict:
    """
    Fetches a coarse DEM mosaic covering a bounding box from the AWS terrain tiles.

    Terrarium PNG tiles encode elevation as ``R * 256 + G + B / 256 - 32768`` meters. The mosaic is
    cropped to the bounding box; latitudes follow image order (north to south) and are slightly
    non-uniform because of the web-mercator projection, which is irrelevant for the nearest-neighbor
    sampling this DEM is used for.

    :param bbox: The bounding box as (min_lon, min_lat, max_lon, max_lat).
    :type bbox: List[float]
    :param zoom: The tile zoom level, defaults to 9 (~230 m pixels at mid-latitudes).
    :type zoom: int, optional
    :return: A dict with "elevation_m" (2-D array), "lats" (per-row, descending) and "lons" (per-col).
    :rtype: Dict
    """
    min_lon, min_lat, max_lon, max_lat = bbox
    x0, y0 = _tile_indices(max_lat, min_lon, zoom)
    x1, y1 = _tile_indices(min_lat, max_lon, zoom)
    from PIL import Image
    rows = []
    for y in range(y0, y1 + 1):
        tiles = []
        for x in range(x0, x1 + 1):
            response = requests.get(TERRARIUM_TILE_URL.format(z=zoom, x=x, y=y), timeout=120)
            response.raise_for_status()
            tiles.append(np.asarray(Image.open(io.BytesIO(response.content)).convert("RGB"),
                                    dtype=np.float64))
        rows.append(np.concatenate(tiles, axis=1))
    mosaic = np.concatenate(rows, axis=0)
    elevation = mosaic[:, :, 0] * 256.0 + mosaic[:, :, 1] + mosaic[:, :, 2] / 256.0 - 32768.0
    n_pixels = 2 ** zoom * TILE_SIZE
    cols = x0 * TILE_SIZE + np.arange(mosaic.shape[1]) + 0.5
    rows_idx = y0 * TILE_SIZE + np.arange(mosaic.shape[0]) + 0.5
    lons = cols / n_pixels * 360.0 - 180.0
    lats = np.degrees(np.arctan(np.sinh(math.pi * (1.0 - 2.0 * rows_idx / n_pixels))))
    keep_rows = (lats >= min_lat) & (lats <= max_lat)
    keep_cols = (lons >= min_lon) & (lons <= max_lon)
    return {"elevation_m": elevation[np.ix_(keep_rows, keep_cols)],
            "lats": lats[keep_rows], "lons": lons[keep_cols]}


def sample_dem_at(dem: Dict, lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
    """
    Samples a DEM at the cross-product of grid latitudes/longitudes by nearest neighbor.

    :param dem: A DEM dict as returned by :func:`sample_dem_grid`.
    :type dem: Dict
    :param lats: The 1-D target cell-center latitudes.
    :type lats: np.ndarray
    :param lons: The 1-D target cell-center longitudes.
    :type lons: np.ndarray
    :return: Elevations in meters with shape (len(lats), len(lons)).
    :rtype: np.ndarray
    """
    lat_idx = np.abs(np.asarray(dem["lats"], dtype=float)[:, None] -
                     np.asarray(lats, dtype=float)[None, :]).argmin(axis=0)
    lon_idx = np.abs(np.asarray(dem["lons"], dtype=float)[:, None] -
                     np.asarray(lons, dtype=float)[None, :]).argmin(axis=0)
    return dem["elevation_m"][np.ix_(lat_idx, lon_idx)]


def band_mean_swe(swe_mm: np.ndarray, lats: np.ndarray, lons: np.ndarray, geometry: Dict,
                  band_elevations_m: List[float], dem: Optional[Dict] = None,
                  zoom: int = 9) -> Dict:
    """
    Computes per-elevation-band mean SWE for a basin from a gridded SWE field.

    Band elevations are typically the equal-area band midpoints from
    ``swe_assimilation.equal_area_bands``; band edges are placed halfway between consecutive
    midpoints (the outer bands are open-ended), each in-basin grid cell is assigned to a band by its
    DEM elevation, and NaN SWE cells are excluded from the means.

    :param swe_mm: The 2-D SWE field in millimeters (NaN where missing).
    :type swe_mm: np.ndarray
    :param lats: The 1-D cell-center latitudes of the SWE grid.
    :type lats: np.ndarray
    :param lons: The 1-D cell-center longitudes of the SWE grid.
    :type lons: np.ndarray
    :param geometry: The basin GeoJSON polygon (Features are unwrapped).
    :type geometry: Dict
    :param band_elevations_m: The band midpoint elevations in meters, ascending.
    :type band_elevations_m: List[float]
    :param dem: An optional pre-fetched DEM dict from :func:`sample_dem_grid` (fetched over the
        basin bounding box when None), defaults to None.
    :type dem: Dict, optional
    :param zoom: The DEM tile zoom level when fetching, defaults to 9.
    :type zoom: int, optional
    :return: A dict with "band_elevations_m", "band_mean_swe_mm" (NaN for bands with no cells),
        "band_cell_counts" and "basin_mean_swe_mm".
    :rtype: Dict
    """
    from usgs_scraping_functions import basin_bounding_box
    geometry = normalize_geometry(geometry)
    band_elevations = np.asarray(band_elevations_m, dtype=float)
    if np.any(np.diff(band_elevations) <= 0):
        raise ValueError("band_elevations_m must be strictly ascending")
    if dem is None:
        dem = sample_dem_grid(list(basin_bounding_box(geometry, buffer_degrees=0.05)), zoom=zoom)
    mask = polygon_mask(lats, lons, geometry)
    cell_elevation = sample_dem_at(dem, lats, lons)
    swe = np.asarray(swe_mm, dtype=float)
    valid = mask & ~np.isnan(swe)
    edges = (band_elevations[:-1] + band_elevations[1:]) / 2.0
    band_index = np.digitize(cell_elevation, edges)
    means, counts = [], []
    for band in range(len(band_elevations)):
        cells = valid & (band_index == band)
        counts.append(int(cells.sum()))
        means.append(float(swe[cells].mean()) if cells.any() else float("nan"))
    basin_mean = float(swe[valid].mean()) if valid.any() else float("nan")
    return {"band_elevations_m": band_elevations.tolist(), "band_mean_swe_mm": means,
            "band_cell_counts": counts, "basin_mean_swe_mm": basin_mean}
