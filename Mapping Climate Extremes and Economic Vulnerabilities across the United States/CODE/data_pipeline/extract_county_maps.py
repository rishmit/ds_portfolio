import tempfile, os, requests, geopandas as gpd
import pandas as pd
import numpy as np
from tqdm import tqdm

def county_url(year: int):
    """
    Args:
        year: relevant year for county maps

    Returns:
        Path to most appropriate simplified county map from US census.gov
    """

    if year < 2000:
        raise ValueError("Supported range is 2000–present.")

    # elif year < 2007:
    #     return "https://www2.census.gov/geo/tiger/PREVGENZ/co/co99_d00_shp.zip"
    # The above file has been removed :(

    elif year < 2013:
        return "https://www2.census.gov/geo/tiger/GENZ2010/gz_2010_us_050_00_500k.zip"

    else:
        return f"https://www2.census.gov/geo/tiger/GENZ{year}/shp/cb_{year}_us_county_500k.zip"


def read_county_geom(year: int, usecols=None):
    """
    Streams the ZIP and returns a GeoDataFrame.
    """
    url = county_url(year)
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()

    with tempfile.TemporaryDirectory() as d:
        zpath = os.path.join(d, f"county_{year}.zip")
        with open(zpath, "wb") as f:
            f.write(resp.content)
        gdf = gpd.read_file(f"zip://{zpath}", usecols=usecols)

    if 'GEOID' not in gdf.columns:
        gdf['GEOID'] = gdf['STATE'] + gdf['COUNTY']

    gdf = gdf[['GEOID', 'NAME', 'geometry']]

    return gdf


def get_all_county_geoms():
    geom_legacy = read_county_geom(year=2010)
    geom_legacy['year'] = 2010

    all_years = np.arange(2014, 2025)
    annual_county_geoms = pd.DataFrame()
    for year in tqdm(all_years, total=len(all_years), unit="year", desc="Loading Geometry maps"):
        geoms = read_county_geom(year=year)
        geoms['year'] = year
        annual_county_geoms = pd.concat([annual_county_geoms, geoms], axis=0)

    all_county_geoms = pd.concat([annual_county_geoms, geom_legacy], axis=0)

    return all_county_geoms


if __name__ == "__main__":
    # counties_2018 = read_counties(2018)
    # counties_2010 = read_counties(year=2010)  # legacy gz_2010
    # counties_2000 = read_counties(2000)  # legacy 2000 pattern
    all_geoms = get_all_county_geoms()
    print(all_geoms['year'].value_counts())
