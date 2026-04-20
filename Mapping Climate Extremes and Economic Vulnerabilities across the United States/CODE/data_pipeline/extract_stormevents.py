import io, requests
import pandas as pd
import numpy as np
import re
from extract_climate_zones import load_geom_map
from tqdm import tqdm


# DOCS: https://www.ncei.noaa.gov/access/search/data-search/storm-data-publication?pageSize=10&pageNum=1&startDate=2000-01-01T00:00:00&endDate=2025-01-01T23:59:59
# GPT Chat: https://chatgpt.com/c/68d2a855-ef38-8327-aac2-0661511418c6


# fips_deltas = pd.read_csv('FIPS_delta_map.csv')
geom_map = load_geom_map()

page_index = requests.get("https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/").text
def get_storm_data_by_year(year: int, index=page_index) -> pd.DataFrame:
    # search for CSVs that match {year}
    m = re.search(fr"StormEvents_details-ftp_v1\.0_d{year}_c\d+\.csv\.gz", index)
    # take 1st (latest) file per year
    fname = m.group(0)
    # extract data from CSV
    file_url = f"https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/{fname}"
    r = requests.get(file_url, stream=True)
    r.raise_for_status()

    events_df = pd.read_csv(io.BytesIO(r.content), compression="gzip", low_memory=False)

    return events_df


def assign_geoid(df):

    df = df[~df['STATE_FIPS'].isnull()]
    df = df[~df['CZ_FIPS'].isnull()]
    df['STATE_FIPS'] = df['STATE_FIPS'].astype(int).astype(str).str.zfill(2)
    df['CZ_FIPS'] = df['CZ_FIPS'].astype(int).astype(str).str.zfill(3)
    df['GEOID'] = df['STATE_FIPS'] + df['CZ_FIPS']

    county_events = df.query('CZ_TYPE == "C"')
    zone_events = df.query('CZ_TYPE != "C"')
    zone_events = zone_events.merge(geom_map, how="inner", left_on='GEOID', right_on='GEOID_zone')
    zone_events['GEOID'] = zone_events['GEOID_county']
    zone_events.drop(columns=['GEOID_zone', 'GEOID_county'], inplace=True)

    storm_events_final = pd.concat([county_events, zone_events], axis=0)
    return storm_events_final


def parse_damages(x):

    _CURRENCY_WORDS = re.compile(r'(?i)\b(USD|EUR|GBP|CAD|AUD|JPY|CNY|RMB)\b')
    _ALLOWED = re.compile(r'^\s*([+-]?\d*\.?\d+(?:[eE][+-]?\d+)?)\s*([a-zA-Z\.]+)?\s*$')
    _MULTIPLIERS = {
        "K": 1e3, "THOUSAND": 1e3,
        "M": 1e6, "MM": 1e6, "MN": 1e6, "MILLION": 1e6,
        "B": 1e9, "BN": 1e9, "BILLION": 1e9,
        "T": 1e12, "TRILLION": 1e12,
    }

    # missing / empty
    if pd.isna(x):
        return 0.0
    s = str(x).strip()
    if s == "":
        return 0.0

    # handle (negative)
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1].strip()

    # strip currency symbols/words and formatting noise
    s = s.replace(",", "")
    s = s.replace("$", "").replace("€", "").replace("£", "")
    s = _CURRENCY_WORDS.sub("", s).strip()

    # match number + optional suffix (case-insensitive)
    m = _ALLOWED.match(s)
    if not m:
        return 0.0

    num = float(m.group(1))
    suf = (m.group(2) or "").strip().upper().rstrip(".")

    # normalize obvious leading-letter cases if not in dictionary
    if suf in _MULTIPLIERS:
        num *= _MULTIPLIERS[suf]
    elif suf:
        if   suf.startswith("K"): num *= 1e3
        elif suf.startswith("M"): num *= 1e6
        elif suf.startswith("B"): num *= 1e9
        elif suf.startswith("T"): num *= 1e12
        elif suf in {"THOU","THOUS"}: num *= 1e3
        elif suf in {"MILL","MILLS"}: num *= 1e6
        elif suf in {"BILL","BILLS"}: num *= 1e9
        elif suf in {"TRILL","TRILLS"}: num *= 1e12
        else:
            # unrecognized trailing text → treat as invalid
            return np.nan

    if neg:
        num = -num

    return num


def inflation_adjuster(year, dollars):
    conversion_dict = {
        '2000': 1.867,
        '2001': 1.815,
        '2002': 1.787,
        '2003': 1.747,
        '2004': 1.702,
        '2005': 1.646,
        '2006': 1.595,
        '2007': 1.550,
        '2008': 1.493,
        '2009': 1.498,
        '2010': 1.474,
        '2011': 1.429,
        '2012': 1.400,
        '2013': 1.380,
        '2014': 1.358,
        '2015': 1.356,
        '2016': 1.339,
        '2017': 1.311,
        '2018': 1.280,
        '2019': 1.257,
        '2020': 1.242,
        '2021': 1.186,
        '2022': 1.098,
        '2023': 1.055,
        '2024': 1.025,
        '2025': 1.000
    }
    conv_rate = conversion_dict[year]
    return dollars * conv_rate


def create_monthly_loc_events(df):
    df["DAMAGE_PROPERTY"] = df["DAMAGE_PROPERTY"].apply(parse_damages)
    df["DAMAGE_CROPS"] = df["DAMAGE_CROPS"].apply(parse_damages)
    df["financial_damage"] = df["DAMAGE_PROPERTY"] + df["DAMAGE_CROPS"]

    df["injuries"] = df["INJURIES_DIRECT"] + df["INJURIES_INDIRECT"]
    df["deaths"] = df["DEATHS_DIRECT"] + df["DEATHS_INDIRECT"]

    df['NOAA_description'] = df['EPISODE_NARRATIVE'].fillna('') + ' ' + df['EVENT_NARRATIVE'].fillna('')
    df['NOAA_description'] = df['NOAA_description'].str.strip()

    df['date'] = pd.to_datetime(df['BEGIN_DATE_TIME'], format='%d-%b-%y %H:%M:%S', errors='coerce')
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month

    noaa_event_mapping = {
        'Thunderstorm': 'Hurricane/Storm',
        'Thunderstorm Wind': 'Hurricane/Storm',
        'Hail': 'Hurricane/Storm',
        'Winter Storm': 'Snow/Ice/Cold',
        'Winter Weather': 'Snow/Ice/Cold',
        'Drought': 'Drought',
        'High Wind': 'Hurricane/Storm',
        'Flash Flood': 'Flood/Ocean',
        'Heavy Snow': 'Snow/Ice/Cold',
        'Flood': 'Flood/Ocean',
        'Heat': 'Heat',
        'Tornado': 'Tornado',
        'Strong Wind': 'Hurricane/Storm',
        'Heavy Rain': 'Hurricane/Storm',
        'Excessive Heat': 'Heat',
        'Extreme Cold/Wind Chill': 'Snow/Ice/Cold',
        'Dense Fog': 'Other',
        'Frost/Freeze': 'Snow/Ice/Cold',
        'Blizzard': 'Snow/Ice/Cold',
        'Lightning': 'Hurricane/Storm',
        'Cold/Wind Chill': 'Snow/Ice/Cold',
        'High Surf': 'Flood/Ocean',
        'Ice Storm': 'Snow/Ice/Cold',
        'Wildfire': 'Fire/Smoke',
        'Funnel Cloud': 'Tornado',
        'Tropical Storm': 'Hurricane/Storm',
        'Coastal Flood': 'Flood/Ocean',
        'Lake-Effect Snow': 'Snow/Ice/Cold',
        'Dust Storm': 'Other',
        'Storm Surge/Tide': 'Flood/Ocean',
        'Debris Flow': 'Flood/Ocean',
        'Rip Current': 'Flood/Ocean',
        'Hurricane (Typhoon)': 'Hurricane/Storm',
        'Avalanche': 'Snow/Ice/Cold',
        'Sleet': 'Hurricane/Storm',
        'Astronomical Low Tide': 'Flood/Ocean',
        'Tropical Depression': 'Hurricane/Storm',
        'Freezing Fog': 'Snow/Ice/Cold',
        'Waterspout': 'Tornado',
        'Lakeshore Flood': 'Flood/Ocean',
        'Dust Devil': 'Other',
        'Dense Smoke': 'Fire/Smoke',
        'Seiche': 'Flood/Ocean',
        'Volcanic Ashfall': 'Fire/Smoke',
        'Sneakerwave': 'Flood/Ocean',
        'Tsunami': 'Flood/Ocean',
        'Hurricane': 'Hurricane/Storm',
        'Northern Lights': 'Other',
        'Volcanic Ash': 'Fire/Smoke',
        'Marine Strong Wind': 'Hurricane/Storm',
        'Marine High Wind': 'Hurricane/Storm',
        'Marine Thunderstorm Wind': 'Hurricane/Storm',
    }
    df['event_type'] = df['EVENT_TYPE'].replace(noaa_event_mapping).fillna('Other')

    us_territories = ['VIRGIN ISLANDS', 'AMERICAN SAMOA', 'GUAM', 'PUERTO RICO']
    df = df[~df['STATE'].isin(us_territories)]

    non_state_entities = [
        'GULF OF MEXICO',
        'ATLANTIC SOUTH',
        'ATLANTIC NORTH',
        'LAKE MICHIGAN',
        'LAKE ERIE',
        'LAKE SUPERIOR',
        'LAKE ST CLAIR',
        'LAKE ONTARIO',
        'LAKE HURON',
        'E PACIFIC',
        'ST LAWRENCE R',
        'GULF OF ALASKA',
        'GUAM WATERS',
    ]
    df = df[~df['STATE'].isin(non_state_entities)]

    monthly_loc_events_df = df.groupby(["GEOID", "year", "month", "event_type"]).agg({
        "deaths": "sum",
        "injuries": "sum",
        "financial_damage": "sum",
        'NOAA_description': '\n'.join
    }).reset_index()

    monthly_loc_events_df.columns = [
        "GEOID", "year", "month", "event_type",
        "deaths", "injuries", "financial_damage", "NOAA_description"
    ]

    return monthly_loc_events_df


def extract_events_for_years(start_year: int = 2000, end_year: int = 2025) -> pd.DataFrame:

    all_years = np.arange(start_year, end_year + 1)
    all_events = pd.DataFrame()
    raw_rows = 0

    total_memory = 0
    for year in tqdm(all_years, total=len(all_years), unit="year", desc="Loading STORMEVENTS"):
        df = get_storm_data_by_year(year=year)
        annual_file_memory = df.memory_usage(deep=True).sum()
        total_memory += annual_file_memory
        raw_rows += len(df)

        # Create and Map Nearest County GEOID
        df = assign_geoid(df)

        # Map specific FIPS not in geom file to their 2025 equivalent
        fips_deltas = {
            "15002": "15005",
            "15008": "15001",
            "15020": "15001",
            "15013": "15001",
            "15012": "15001",
            "06090": "06077",
            "15019": "15009",
            "48258": "48109",
            "06092": "06077",
            "15025": "15001",
            "30048": "30047",
            "06096": "06099",
            "30012": "30013",
            "30010": "30035",
            "15024": "15009",
            "51052": "51153",
            "49008": "49051",
            "42068": "42091",
            "41002": "41057",
            "17014": "17031",
            "30014": "30013",
            "06054": "06037",
            "30050": "30045",
            "02213": "02180",
            "02025": "02110",
            "50012": "50027",
            "06098": "06071",
            "02019": "02110",
            "02101": "02020",
            "02155": "02050",
            "51042": "51107",
            "53042": "53047",
            "30044": "30101",
            "02125": "02122",
            "15021": "15009",
            "35523": "35001",
            "49002": "49003",
            "02018": "02105",
            "02131": "02261",
            "02027": "02130",
            "24007": "24025",
            "02028": "02100",
            "48074": "48123",
            "53503": "53073",
            "24010": "24027",
            "02181": "02164",
            "15014": "15009",
            "35526": "35001",
            "35528": "35007",
            "35529": "35007",
            "22062": "22071",
            "41006": "41051",
            "28080": "28045",
            "6.0073": "06073",
            "6.0055": "06071",
            "36076": "36081",
            "28082": "28059",
            "47.0113": "47113",
            "12036": "12001",
            "12062": "12015",
            "53508": "53033",
            "13.0205": "13205",
            "24.0017": "24037",
            "40.0109": "40109",
            "19.0027": "19027",
            "49004": "49035",
            "35519": "35001",
            "51.0037": "51003",
            "48.0157": "48157",
            "12002": "12033",
            "22040": "22103",
            "41010": "41005",
            "48216": "48361",
            "12064": "12085",
            "12054": "12061",
            "1.0073": "01073",
            "22042": "22053",
            "6.0071": "06071",
            "12026": "12077",
            "37.0103": "37055",
            "22072": "22105",
            "22050": "22063",
            "24.0011": "24005",
            "22052": "22113",
            "48.0021": "48369",
            "40.0027": "40027",
            "41012": "41039",
            "06192": "06019",
            "22054": "22101",
            "40.0109": "40109",
            "51.0094": "51700",
            "24.0006": "24005",
            "06042": "06059",
            "51.0091": "51199",
            "11.0001": "11001",
            "35540": "35005",
            "26.0069": "26125",
            "39.0153": "39153",
            "35538": "35005",
            "35536": "35041",
            "35535": "35009",
        }
        df['GEOID'] = df['GEOID'].map(lambda x: fips_deltas.get(x, x))

        monthly_loc_events_df = create_monthly_loc_events(df)
        monthly_loc_events_df['financial_damage'] = inflation_adjuster(str(year), monthly_loc_events_df['financial_damage'])

        # Merge County POLYGONS
        # all_geoms['year_diff'] = np.abs(all_geoms['year'] - year)
        # latest_geoms = all_geoms.sort_values('year_diff').drop_duplicates(subset='GEOID')
        # daily_loc_events_df = daily_loc_events_df.merge(latest_geoms[['GEOID', 'NAME', 'geometry']], how='left', on='GEOID')

        all_events = pd.concat([all_events, monthly_loc_events_df], axis=0)
        # print(f"Year: {year} | New Rows: {len(monthly_loc_events_df):,} | All Rows: {len(all_events):,}")

    print(f"Raw rows downloaded: {raw_rows:,} | Total Raw File Size: {total_memory/1000000000:.2f} Gb")

    return all_events



# all_events = extract_events_for_years(2000, 2025)
# all_events.to_csv("storm_events.csv", index=False)


# missing_geom = all_events[all_events['geometry'].isnull()].copy()
# test = missing_geom.groupby(['GEOID', 'state', 'cz_name', 'cz_type']).count().reset_index()
# missing_pop = all_events[~all_events['GEOID'].isin(census_pops_df['GEOID'])]
# test = missing_pop.groupby(['GEOID', 'state', 'cz_name', 'cz_type']).count().reset_index()


# print(df.columns)
# print(df['EVENT_TYPE'].value_counts().head(10))
# print(df['DATA_SOURCE'].value_counts().head(10))
# print(df['DEATHS_DIRECT'].value_counts().head(10))
# print(df['BEGIN_LOCATION'].value_counts().head(10))

