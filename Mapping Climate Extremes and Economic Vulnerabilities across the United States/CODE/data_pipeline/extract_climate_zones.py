import os, re, tempfile, io, requests
import geopandas as gpd
import pandas as pd
from extract_county_maps import read_county_geom


HEADERS = {"User-Agent": "python-requests (contact: you@example.com)"}

def _latest_zip_url(page_url: str, pattern: str):
    """Fetch an NWS page and return the newest matching .zip URL by regex."""
    html = requests.get(page_url, headers=HEADERS, timeout=60)
    html.raise_for_status()
    # Find *all* zip links that match the pattern (e.g., z_ddmmyy.zip)
    links = re.findall(pattern, html.text, flags=re.I)
    if not links:
        raise RuntimeError(f"No ZIP links matched on {page_url}")
    url = links[-1]  # last is typically the newest on these pages
    if url.startswith("/"):
        url = "https://www.weather.gov" + url
    return url


def _read_zip_to_gdf(url: str, usecols=None):
    """Download a ZIP to a temp file and open with GDAL via zip:// scheme."""
    with tempfile.TemporaryDirectory() as td:
        zpath = os.path.join(td, os.path.basename(url) or "zones.zip")
        r = requests.get(url, headers=HEADERS, timeout=120)
        r.raise_for_status()
        # Quick sanity: make sure we actually got a ZIP (sometimes a blocked HTML page sneaks in)
        if r.content[:2] != b"PK":
            raise RuntimeError(f"Expected a ZIP from {url}, got non-ZIP content (len={len(r.content)})")
        with open(zpath, "wb") as f:
            f.write(r.content)
        # Let GDAL figure out the .shp inside the archive
        return gpd.read_file(f"zip://{zpath}", usecols=usecols)


def read_public_zones_latest(usecols=None):
    """
    Public Forecast Zones (land). NWS publishes z_ddmmyy.zip.
    Returns a GeoDataFrame with columns like STATE, ZONE, STATE_ZONE, NAME, geometry.
    """
    url = _latest_zip_url(
        page_url = "https://www.weather.gov/gis/PublicZones",
        pattern = r'href="([^"]*z_[0-9a-z]{6}\.zip)"'
    )

    state_to_fips = {
        'AL': '01', 'AK': '02', 'AZ': '04', 'AR': '05', 'CA': '06', 'CO': '08', 'CT': '09', 'DC': '11', 'DE': '10',
        'FL': '12', 'GA': '13', 'HI': '15', 'IA': '19', 'ID': '16', 'IL': '17', 'IN': '18', 'KS': '20', 'KY': '21',
        'LA': '22', 'MA': '25', 'MD': '24', 'ME': '23', 'MI': '26', 'MN': '27', 'MO': '29', 'MS': '28', 'MT': '30',
        'NC': '37', 'ND': '38', 'NE': '31', 'NH': '33', 'NJ': '34', 'NM': '35', 'NV': '32', 'NY': '36', 'OH': '39',
        'OK': '40', 'OR': '41', 'PA': '42', 'PR': '72', 'RI': '44', 'SC': '45', 'SD': '46', 'TN': '47', 'TX': '48',
        'UT': '49', 'VA': '51', 'VI': '78', 'VT': '50', 'WA': '53', 'WI': '55', 'WV': '54', 'WY': '56',
        'GU': '66', 'AS': '60', 'MP': '69'  # territories if present
    }

    zones_df = _read_zip_to_gdf(url, usecols=usecols)
    # zones: your NWS Public Forecast Zones GeoDataFrame
    zones_df["STATE_FIPS"] = zones_df["STATE"].map(state_to_fips)
    zones_df["ZONE_CODE"] = zones_df["ZONE"].astype(str).str.zfill(3)
    zones_df["GEOID"] = zones_df["STATE_FIPS"] + zones_df["ZONE_CODE"]

    zones_df = zones_df[['GEOID', 'NAME', 'LON', 'LAT', 'geometry']]

    return zones_df


def _read_water_zone_id_map():
    url = "https://www.weather.gov/gis/EasNWR"
    html = requests.get(url, timeout=60).text
    # pick the most recent "Coastal & Offshore Marine Area & Zone Codes for EAS" file (mareasddmmyy.txt)
    links = re.findall(r'href="([^"]*mareas[0-9a-z]{6}\.txt)"', html, flags=re.I)
    if not links:
        raise RuntimeError("Could not find mareas*.txt on NWS EAS/NWR page.")
    codes_url = ("https://www.weather.gov" + links[-1]) if links[-1].startswith("/") else links[-1]

    txt = requests.get(codes_url, timeout=120).text

    df_codes = pd.read_csv(io.StringIO(txt), sep="|", dtype=str, header=None)
    df_codes.columns = ["zone_prefix", "GEOID", "zone_name", "LAT", "LON"]
    df_codes['ID'] = df_codes['zone_prefix'] + 'Z' + df_codes['GEOID'].str[-3:]
    df_codes = df_codes[['GEOID', 'ID']]

    return df_codes


def read_marine_zones_latest(kind="coastal", usecols=None):
    """
    Marine zones:
      kind='coastal'  -> mzddmmyy.zip
      kind='offshore' -> ozddmmyy.zip
      kind='highseas' -> hzddmmyy.zip
    """
    pat = {
        "coastal":  r'href="([^"]*mz[0-9a-z]{6}\.zip)"',
        "offshore": r'href="([^"]*oz[0-9a-z]{6}\.zip)"',
        "highseas": r'href="([^"]*hz[0-9a-z]{6}\.zip)"',
    }[kind]
    url = _latest_zip_url("https://www.weather.gov/gis/MarineZones", pat)
    water_zones = _read_zip_to_gdf(url, usecols=usecols)

    geo_id_map = _read_water_zone_id_map()
    water_zones = water_zones.merge(geo_id_map, how='inner', on='ID')
    water_zones = water_zones[['GEOID', 'NAME', 'LON', 'LAT', 'geometry']]

    return water_zones


def load_county_geoms():
    latest_county_geoms = read_county_geom(year=2024)
    county_zones_proj = latest_county_geoms.to_crs("EPSG:5070")
    return county_zones_proj


def map_zone_to_county(geom_zones, county_zones_proj):
    # Calculate centroids of climate zones
    geom_zones_proj = geom_zones.to_crs("EPSG:5070")  # NAD83 / Conus Albers
    geom_zones_proj['centroid'] = geom_zones_proj.geometry.centroid
    climate_points = geom_zones_proj.set_geometry('centroid')

    # Nearest join in projected space
    nearest = gpd.sjoin_nearest(climate_points, county_zones_proj, how="left", distance_col="dist_to_county")

    # (Optional) Convert back to WGS84 for mapping
    mapping = nearest[['GEOID_left', 'GEOID_right']]
    mapping.columns = ['GEOID_zone', 'GEOID_county']
    return mapping


def load_geom_map():
    geom_zones = read_public_zones_latest(usecols=None)
    geom_coastal = read_marine_zones_latest("coastal", usecols=None)
    geom_offshore = read_marine_zones_latest("offshore", usecols=None)

    county_zones_proj = load_county_geoms()

    geom_zone_mapping = map_zone_to_county(geom_zones, county_zones_proj)
    geom_coastal_mapping = map_zone_to_county(geom_coastal, county_zones_proj)
    geom_offshore_mapping = map_zone_to_county(geom_offshore, county_zones_proj)
    geom_mapping = pd.concat([geom_zone_mapping, geom_coastal_mapping, geom_offshore_mapping], axis=0)

    return geom_mapping.drop_duplicates(subset='GEOID_zone')


if __name__ == "__main__":
    geom_mapping = load_geom_map()
