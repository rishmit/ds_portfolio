from census import Census
from us import states
import pandas as pd
import time
import requests
from tqdm import tqdm

API_KEY = 12345 #[Request and Insert your API Key here]#
c = Census(API_KEY)

def get_annual_pops():
    all_data = []
    acs_years = list(range(2009, 2024))

    for year in tqdm(acs_years, total=len(acs_years), unit='year', desc='Fetching Annual County Populations:'):
        # print(f"Fetching ACS 5-year data for {year}...")
        for state in states.STATES:
            try:
                rows = c.acs5.state_county(
                    ('NAME', 'B01003_001E'),  # Total population
                    state.fips,
                    '*',
                    year=year
                )
                for r in rows:
                    all_data.append({
                        "year": year,
                        "state_fips": state.fips,
                        "state": state.name,
                        "county_fips": r["county"],
                        "county_name": r["NAME"],
                        "population": r["B01003_001E"]
                    })
            except Exception as e:
                print(f"Error fetching {state.name} ({year}): {e}")
                time.sleep(2)


    census_annual_df = pd.DataFrame(all_data)
    census_annual_df['GEOID'] = census_annual_df['state_fips'] + census_annual_df['county_fips']
    return census_annual_df


def fetch_decennial(year):
    url = f"https://api.census.gov/data/{year}/dec/sf1"
    params = {
        "get": "NAME,P001001",
        "for": "county:*",
        "key": API_KEY
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    return pd.DataFrame(data[1:], columns=data[0]).assign(year=year)


def get_county_populations():
    dec_2000 = fetch_decennial(2000)
    dec_2010 = fetch_decennial(2010)
    dec_2000.rename(columns={"P001001": "population"}, inplace=True)
    dec_2010.rename(columns={"P001001": "population"}, inplace=True)

    census_decennial_df = pd.concat([dec_2000, dec_2010])
    census_decennial_df['GEOID'] = census_decennial_df['state'] + census_decennial_df['county']

    census_annual_df = get_annual_pops()

    # Combine
    census_pops_df = pd.concat([
        census_annual_df[['GEOID', 'year', 'population']],
        census_decennial_df[['GEOID', 'year', 'population']]
    ])

    return census_pops_df

# census_pops_df.to_csv("census_pops.csv", index=False)
