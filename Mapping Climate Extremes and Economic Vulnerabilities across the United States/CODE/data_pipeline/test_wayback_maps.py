import os, re, time, json, pathlib, requests
from urllib.parse import quote

import os, re, tempfile, io, requests
import geopandas as gpd
import pandas as pd
from tqdm import tqdm

SESS = requests.Session()
SESS.headers.update({"User-Agent": "historical-zones-archive (contact: you@example.com)"})

PUBLIC_PAGE = "https://www.weather.gov/gis/PublicZones"
# ZONECOUNTY_PAGE = "https://www.weather.gov/gis/ZoneCounty"


def wayback_snapshots(url):
    # CDX API: list 200-status snapshots (JSON)
    cdx = f"https://web.archive.org/cdx/search/cdx?url={quote(url)}&output=json&filter=statuscode:200&collapse=digest"
    r = SESS.get(cdx, timeout=60)
    r.raise_for_status()
    rows = r.json()
    # first row is header
    return [dict(zip(rows[0], row)) for row in rows[1:]]


def fetch_archived_html(url, ts):
    # Construct archived page URL
    aurl = f"https://web.archive.org/web/{ts}/{url}"
    r = SESS.get(aurl, timeout=60)
    r.raise_for_status()
    return r.text


def extract_links(html, pattern):
    return re.findall(pattern, html, flags=re.I)


def to_archived_file_url(href):
    return f"https://web.archive.org{href}"


def _read_zip_to_gdf(url: str, usecols=None):
    HEADERS = {"User-Agent": "python-requests (contact: you@example.com)"}
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


def find_public_zips(PAGE_URL):
    snaps = wayback_snapshots(PAGE_URL)
    found = {}  # name -> archived_url
    for s in tqdm(snaps, total=len(snaps), unit='snapshot', desc='Searching WayBack versions'):
        ts = s["timestamp"]
        html = fetch_archived_html(PUBLIC_PAGE, ts)
        # match z_ddmmyy.zip links
        pattern = r'href="([^"]*z_[0-9a-z]{6}\.zip)"'
        for href in extract_links(html, pattern):
            name = os.path.basename(href)
            # found.setdefault(name, to_archived_file_url(href))
            found.setdefault(name, href)
        time.sleep(0.2)  # be polite

    print(f"Public zones: {len(found)} versions discovered.")

    return found




if __name__ == "__main__":

    zone_maps_found = find_public_zips(PAGE_URL=PUBLIC_PAGE)
    zone_maps_names = [key for key in zone_maps_found.keys()]
    zone_maps_years = [item.split('.')[0][-2:] for item in zone_maps_names]

    # TODO: 1 file per year, load file and append with year column, integrate with final function

    # first_url_per_year = {}
    all_geom_zones = pd.DataFrame()
    for year, file_name in zip(zone_maps_years, zone_maps_names):
        if (len(all_geom_zones) == 0) or (year not in all_geom_zones['year']):  # only add one file per year
            try:
                file_path = zone_maps_found[file_name]
                geom_zones = _read_zip_to_gdf(url=file_path)
                geom_zones['year'] = int('20' + year)
                all_geom_zones = pd.concat([all_geom_zones, geom_zones], axis=0)
                print(f"New Rows: {len(geom_zones):,} | Total Rows: {len(all_geom_zones):,}")
            except:
                print(f'could not load file from: {file_name}')
    print(all_geom_zones['year'].value_counts())

    # NOTE: The internet archive has over 30 snapshots of the public zones file, but
    # only 12 have a .zip that matches the pattern, and only 2 are actually downloadable.
    # Seems like a dead-end.
