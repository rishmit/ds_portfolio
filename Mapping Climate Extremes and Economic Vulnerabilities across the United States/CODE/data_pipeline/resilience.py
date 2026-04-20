# %%
# ==========================================
# 1. IMPORT LIBRARIES
# ==========================================
import pandas as pd
import numpy as np
from extract_stormevents import extract_events_for_years, inflation_adjuster
from extract_county_populations import get_county_populations

# %%
# ==========================================
# 2. LOAD DATA
# ==========================================
fema_spend = pd.read_csv('../data/pa_funds_total.csv')
storm_events = extract_events_for_years(2000, 2025)
census_pops_df = get_county_populations()  # pd.read_csv('census_pops.csv')
zillow_data = pd.read_csv('../data/zillow_data.csv')
unemp_data = pd.read_csv('../data/Unemployment_2000_2024.csv')

# Get the years in question
all_years = storm_events['year'].unique()

# %%
# ==========================================
# 3. TRANSFORM THE FEMA DATA
# ==========================================
fema_spend['GEOID'] = fema_spend['countyFIPS'].astype(str).str.zfill(5)
fema_spend["GEOID"] = fema_spend["GEOID"].astype(str).str.replace(r"\.(?=\d+$)", "", regex=True).str.strip()
fema_spend["GEOID"] = fema_spend["GEOID"].astype(int)

# %%
# Map incident types to simpler categories
fema_event_mapping = {
    'Severe Storm(s)': 'Hurricane/Storm',
    'Hurricane': 'Hurricane/Storm',
    'Tropical Storm': 'Hurricane/Storm',
    'Coastal Storm': 'Hurricane/Storm',
    'Severe Storms, Straight-line Winds, Tornadoes, and Flooding': 'Hurricane/Storm',
    'Typhoon': 'Hurricane/Storm',
    'Flood': 'Flood/Ocean',
    'Tsunami': 'Flood/Ocean',
    'Dam/Levee Break': 'Flood/Ocean',
    'Snow': 'Snow/Ice/Cold',
    'Severe Ice Storm': 'Snow/Ice/Cold',
    'Winter Storm': 'Snow/Ice/Cold',
    'Freezing': 'Snow/Ice/Cold',
    'Fire': 'Fire/Smoke',
    'Wildfire': 'Fire/Smoke',
    'Tornado': 'Tornado',
    'Earthquake': 'Other',
    'Mud/Landslide': 'Other',
    'Volcano': 'Volcano',
    'Biological': 'non-weather',
    'Terrorist Attack': 'non-weather',
    'Chemical': 'non-weather',
}
fema_spend['event_type'] = fema_spend['incidentType'].replace(fema_event_mapping).fillna('Other')

# %%
# Aggregate FEMA spend by County/Year/Month
fema_weather_spend = fema_spend.query('event_type != "non-weather"')
group_cols = ['GEOID', 'declarationYear', 'declarationMonth']
fema_monthly = fema_weather_spend.groupby(group_cols)['totalFedralShareObligated'].sum().reset_index()
fema_monthly.columns = ['GEOID', 'year', 'month', 'fema_spend']

for year in all_years:
    fema_monthly['fema_spend'] = np.where(
        fema_monthly['year'] == year,
        inflation_adjuster(str(year), fema_monthly['fema_spend']),
        fema_monthly['fema_spend']
    )

# %%
# ==========================================
# 4. TRANSFORM NOAA DATA
# ==========================================

# Month mapping
month_map = {
    1: 'January',
    2: 'February',
    3: 'March',
    4: 'April',
    5: 'May',
    6: 'June',
    7: 'July',
    8: 'August',
    9: 'September',
    10: 'October',
    11: 'November',
    12: 'December'
}
storm_events['month_no'] = storm_events['month'].astype(int)
storm_events['month'] = storm_events['month'].replace(month_map)

# %%
# Pivot to get total damage per county/month/year
storms_pivot_df = storm_events.pivot_table(
    index=['GEOID', 'year', 'month_no', 'month'],
    columns='event_type',
    values='financial_damage',
    aggfunc=sum,
    fill_value=0
).reset_index()

# %%
# Calculate total damage across all event types
storms_pivot_df['total_damage'] = storms_pivot_df.loc[:, storm_events['event_type'].unique()].sum(axis=1)

# Merge descriptions with storm events
storm_descriptions = storm_events.groupby(['GEOID', 'year', 'month_no']).agg({'NOAA_description': '. '.join}).reset_index()
storms_pivot_df = storms_pivot_df.merge(storm_descriptions, on=['GEOID', 'year', 'month_no'], how='left')

# Clean the descriptions
storms_pivot_df['NOAA_description'] = storms_pivot_df['NOAA_description'].fillna('')
storms_pivot_df['NOAA_description'] = storms_pivot_df['NOAA_description'].str.replace('"', "'")
storms_pivot_df['NOAA_description'] = np.where(
    storms_pivot_df['NOAA_description'].str.len() > 1000,
    storms_pivot_df['NOAA_description'].str[0:1000] + "...",
    storms_pivot_df['NOAA_description']
)
storms_pivot_df["GEOID"] = storms_pivot_df["GEOID"].astype(int)

# %%
# Fill Missing Months
date_data = storms_pivot_df[['year', 'month_no']].rename(columns={'month_no': 'month'})[['year', 'month']].assign(day=1)
storms_pivot_df['date'] = pd.to_datetime(date_data)

all_months = pd.date_range(storms_pivot_df['date'].min(), storms_pivot_df['date'].max(), freq='MS')
geoids = storms_pivot_df['GEOID'].unique()

# Create DataFrame with all GEOID - Year - Month combos
full_index = pd.MultiIndex.from_product(
    [geoids, all_months], names=['GEOID', 'date']
)
template = pd.DataFrame(index=full_index).reset_index()
storms_df_final = template.merge(storms_pivot_df, how='left', on=['GEOID', 'date'])

# Redefine Year, Month_no, Month
storms_df_final['year'] = storms_df_final['date'].dt.year
storms_df_final['month_no'] = storms_df_final['date'].dt.month
storms_df_final['month'] = storms_df_final['month_no'].replace(month_map)

# Fill $ columns with 0
weather_damage_cols = storms_df_final.drop(columns=['GEOID', 'year', 'month_no']).select_dtypes(include=np.number).columns
storms_df_final[weather_damage_cols] = storms_df_final[weather_damage_cols].fillna(0)

storms_df_final = storms_df_final.sort_values(['GEOID', 'date'])

# %%
# ==========================================
# 5. TRANSFORM CENSUS DATA
# ==========================================
census_pops_df['GEOID'] = census_pops_df['GEOID'].astype(str).str.zfill(5).astype(int)
census_pops_df['population'] = census_pops_df['population'].astype(int)

# %%
# ==========================================
# 6. TRANSFORM ZILLOW DATA
# ==========================================
# We will be using the column ZHVI_HomeValues for our index calculation. But we see that it has missing values.
# The best thing we can do is for a county, in the time range, we interpolate the values linearly

merge_cols = ['GEOID', 'year', 'month']

zillow_final = zillow_data[merge_cols + ['ZHVI_HomeValues', "Market Heat Index"]].dropna().copy()
zillow_final['GEOID'] = zillow_final['GEOID'].astype(int)
# zillow_final['ZHVI_HomeValues'] = zillow_final['ZHVI_HomeValues'].astype(str).str.replace(r"[$,]", "", regex=True).str.strip()
zillow_final['ZHVI_HomeValues'] = pd.to_numeric(zillow_final['ZHVI_HomeValues'], errors='coerce')
zillow_final['Market Heat Index'] = pd.to_numeric(zillow_final['Market Heat Index'], errors='coerce')
zillow_final = zillow_final.sort_values(merge_cols)

for year in all_years:
    zillow_final['ZHVI_HomeValues'] = np.where(
        zillow_final['year'] == year,
        inflation_adjuster(str(year), zillow_final['ZHVI_HomeValues']),
        zillow_final['ZHVI_HomeValues']
    )


# %%
# ==========================================
# 7. MERGEs
# ==========================================

d3_data = storms_df_final.merge(
    fema_monthly, how='left', on=merge_cols
).merge(
    zillow_final, how='left', on=merge_cols
)

# Merge population by nearest year
d3_data_final = pd.DataFrame()
for year in all_years:
    census_pops_df['year_diff'] = np.abs(census_pops_df['year'] - year)
    latest_pops = census_pops_df.sort_values('year_diff').drop_duplicates(subset='GEOID')
    d3_data_filled_year = d3_data.query('year == @year').merge(latest_pops[['GEOID', 'population']], how='left', on='GEOID')
    d3_data_final = pd.concat([d3_data_final, d3_data_filled_year], axis=0)
d3_data_final = d3_data_final.sort_values(['GEOID', 'date'])

# ==========================================
# ADD UNEMPLOYMENT DATA
# ==========================================

# Keep only needed columns
unemp_data = unemp_data[['GEOID', 'year', 'Unemployment Rate']].copy()

# Ensure GEOID is consistently padded
unemp_data['GEOID'] = (
    unemp_data['GEOID']
    .astype(str)
    .str.zfill(5)
    .astype(int)
)

# --- Direct merge on exact GEOID/year ---
d3_data_final = d3_data_final.merge(
    unemp_data,
    how='left',
    on=['GEOID', 'year']
)

# --- Fill missing unemployment values using rounded GEOID (e.g., 12031 → 12000) ---
mask = d3_data_final['Unemployment Rate'].isna()

# Build lookup table
lookup = unemp_data.set_index(['GEOID', 'year'])['Unemployment Rate']

# Round to nearest thousand
rounded_geoids = (d3_data_final.loc[mask, 'GEOID'] // 1000) * 1000
years = d3_data_final.loc[mask, 'year']

# Look up fallback values
filled_values = [
    lookup.get((g, y), np.nan)
    for g, y in zip(rounded_geoids, years)
]

# Fill them
d3_data_final.loc[mask, 'Unemployment Rate'] = filled_values


# %%
# ==========================================
# 8. Forward - Backward Fills
# ==========================================
d3_data_final['NOAA_description'] = np.where(
    d3_data_final['NOAA_description'].isnull(),
    np.nan,
    'In ' + d3_data_final['month'] + ' of ' + d3_data_final['year'].astype(str) + ', ' + d3_data_final['NOAA_description']
    )
d3_data_final['NOAA_description'] = d3_data_final.groupby('GEOID')['NOAA_description'].ffill().bfill()

def fill_na_vals(data, col):
    filled_col = data.groupby('GEOID')[col].transform(lambda x: x.interpolate(method='linear')).ffill().bfill()
    return filled_col


d3_data_final['population'] = fill_na_vals(d3_data_final, col='population')
d3_data_final['ZHVI_HomeValues'] = fill_na_vals(d3_data_final, col='ZHVI_HomeValues')
d3_data_final['Market Heat Index'] = fill_na_vals(d3_data_final, col='Market Heat Index')

d3_data_final['Unemployment Rate'] = (
    d3_data_final.groupby('GEOID')['Unemployment Rate']
    .ffill()
    .bfill()
)

# %%
# ==========================================
# 9. ROLLING SUMS
# ==========================================
# Create Trailing-12-month sums
def ttm_sum(data, col):
    ttm = (
        data.groupby('GEOID')[col]
          .rolling(window=12, min_periods=1)
          .sum()
          .reset_index(level=0, drop=True)
    )
    return ttm


for col in weather_damage_cols:
    d3_data_final[col] = ttm_sum(d3_data_final, col=col)

d3_data_final['fema_spend'] = d3_data_final['fema_spend'].fillna(0)
d3_data_final['fema_spend'] = ttm_sum(d3_data_final, col='fema_spend')

d3_data_final['home_value_change'] = d3_data_final.groupby('GEOID')['ZHVI_HomeValues'].pct_change()
d3_data_final['home_value_change'] = ttm_sum(d3_data_final, col='home_value_change')
d3_data_final['home_value_change'] = d3_data_final['home_value_change'].fillna(0)


# %%
# ==========================================
# 10. NORMALIZATION (per capita)
# ==========================================
# Normalize the damage and fema spend ttm
d3_data_final['damage_per_capita'] = d3_data_final['total_damage'] / d3_data_final['population']
d3_data_final['fema_per_capita'] = d3_data_final['fema_spend'] / d3_data_final['population']



# %%
# ==========================================
# 11. CALCULATE RESILIENCE INDEX
# ==========================================
# d3_data_final['fema_damage_ratio'] = np.where(
#     d3_data_final['total_damage'] == 0,
#     np.nan,  # If there's no damage, FEMA is not relevant
#     d3_data_final['fema_spend'] / d3_data_final['total_damage']
# )
# # Let's just say if FEMA > Damage, then the funding has fully covered the damage cost
# d3_data_final['fema_damage_ratio'] = d3_data_final['fema_damage_ratio'].clip(lower=0, upper=1)



# 1. EXTREME SCORE (Based on Per Capita Damage): The damage number is pretty high, let's just log transform it first
d3_data_final['log_damage'] = np.log10(1 + d3_data_final['damage_per_capita'])
min_dam = d3_data_final['log_damage'].min()
max_dam = d3_data_final['log_damage'].max()
d3_data_final['extreme_score'] = (d3_data_final['log_damage'] - min_dam) / (max_dam - min_dam)

#2. IMPACT SCORE (Based on FEMA vs Damage): Define a ratio called fema_damage_ratio
d3_data_final['fema_damage_ratio'] = np.where(
    d3_data_final['total_damage'] == 0,
    1,  # If there's no damage, FEMA is not needed / relevant
    d3_data_final['fema_spend'] / d3_data_final['total_damage']
)
# Let's just say if FEMA > Damage, then the funding has fully covered the damage cost
d3_data_final['fema_damage_ratio'] = d3_data_final['fema_damage_ratio'].clip(upper=1)
# As per paper, we take 1 minus the ratio when we want resilience
d3_data_final['impact_score'] = 1 - d3_data_final['fema_damage_ratio']

# 3. RECOVERY SCORE (Using home_value_change):
min_rec = d3_data_final['home_value_change'].min()
max_rec = d3_data_final['home_value_change'].max()
d3_data_final['homevalue_score'] = (d3_data_final['home_value_change'] - min_rec) / (max_rec - min_rec)
# d3_data_final['homevalue_score'] = 20 - np.abs(d3_data_final['home_value_change'])

# %%
# We add these scores and rescale to 1
d3_data_final['resilience_index'] = (0.33 * d3_data_final['extreme_score']) + (0.33 * d3_data_final['impact_score']) + (0.33 * d3_data_final['homevalue_score'])
min_res = d3_data_final['resilience_index'].min()
max_res = d3_data_final['resilience_index'].max()
d3_data_final['resilience_index'] = ((d3_data_final['resilience_index'] - min_res) / (max_res - min_res))

# %%
print(d3_data_final.head())

# %%
# ==========================================
# 12. SAVE DATA AS CSV
# ==========================================
d3_data_final.to_csv('../data/resilience.csv', index=False)


# test_load = pd.read_csv('../data/resilience.csv')
# bad_data_test = d3_data_final[d3_data_final['date'].isnull()]