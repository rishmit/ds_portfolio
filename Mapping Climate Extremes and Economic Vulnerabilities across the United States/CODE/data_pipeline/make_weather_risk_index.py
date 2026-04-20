import pandas as pd
import numpy as np
import statsmodels.formula.api as smf


df = pd.read_csv('../data/resilience.csv')
df['date'] = pd.to_datetime(df['date'])
unemp_data = pd.read_csv('../data/Unemployment_2000_2024.csv')
unemp_data.rename(columns={'Unemployment Rate': 'unemploy_rate'}, inplace=True)
df = df.merge(unemp_data, how='left', on=['GEOID', 'year'])


# 1. Identify major storms per county and date.
df['damage_monthly'] = df['damage_per_capita'] - df['damage_per_capita'].shift(1)
df['major_storm_damage'] = df.groupby('GEOID')['damage_monthly'].transform(
    lambda x: x >= 10
)
print(f"Major Storm Events: {np.sum(df['major_storm_damage']):,}")

# 2. Rank storm damage across counties per year
yearly_damage = df.query('month_no == 12').groupby(['year', 'GEOID'], as_index=False)['damage_per_capita'].max()
yearly_damage['rank'] = yearly_damage.groupby('year')['damage_per_capita'].rank(
    method='dense', ascending=False
)
yearly_damage.sort_values(['year', 'rank'], inplace=True)

# 3. Rank FEMA funding across counties per year
yearly_fema = df.query('month_no == 12').groupby(['year', 'GEOID'], as_index=False)['fema_per_capita'].max()
yearly_fema['rank'] = yearly_fema.groupby('year')['fema_per_capita'].rank(
    method='dense', ascending=False
)
yearly_fema.sort_values(['year', 'rank'], inplace=True)

# 4. Rank housing changes following event, per year
storms = df.loc[df['major_storm_damage'], ['GEOID', 'date', 'ZHVI_HomeValues']]
df_future = df[['GEOID', 'date', 'ZHVI_HomeValues']].copy()
df_future['date'] = df_future['date'] - pd.DateOffset(months=12)  # shift backward
df_future = df_future.rename(columns={'ZHVI_HomeValues': 'homeval_after_12m'})

home_val_change = pd.merge(
    storms,
    df_future,
    on=['GEOID', 'date'],
    how='left'
)
home_val_change['year'] = home_val_change['date'].dt.year
home_val_change['homeval_change'] = home_val_change['ZHVI_HomeValues'] - home_val_change['homeval_after_12m']
home_val_change['homeval_pct_change'] = np.abs(home_val_change['homeval_change'] / home_val_change['ZHVI_HomeValues'])
home_val_change['rank'] = home_val_change.groupby('year')['homeval_pct_change'].rank(
    method='dense', ascending=False
)
home_val_change.sort_values(['year', 'rank'], inplace=True)


# 5. Rank unemployment increases following event, per year
storms = df.loc[df['major_storm_damage'], ['GEOID', 'date', 'unemploy_rate']]
df_future = df[['GEOID', 'date', 'unemploy_rate']].copy()
df_future['date'] = df_future['date'] - pd.DateOffset(months=12)  # shift backward
df_future = df_future.rename(columns={'unemploy_rate': 'unemploy_rate_after_12m'})

unemploy_change = pd.merge(
    storms,
    df_future,
    on=['GEOID', 'date'],
    how='left'
)
unemploy_change['year'] = unemploy_change['date'].dt.year
unemploy_change['unemploy_change'] = unemploy_change['unemploy_rate_after_12m'] - unemploy_change['unemploy_rate']
unemploy_change['unemploy_pct_change'] = unemploy_change['unemploy_change'] / unemploy_change['unemploy_rate']
unemploy_change['rank'] = unemploy_change.groupby('year')['unemploy_pct_change'].rank(
    method='dense', ascending=False
)
unemploy_change.sort_values(['year', 'rank'], inplace=True)

# 6. Combine Ranks
yearly_damage['category'] = 'Storm Damage'
yearly_fema['category'] = 'FEMA Spend'
home_val_change['category'] = 'Housing Price Flux'
unemploy_change['category'] = 'Unemployment Increase'
rank_cols = ['year', 'rank', 'GEOID', 'category']

combined_ranks = pd.concat([
    yearly_damage[rank_cols],
    yearly_fema[rank_cols],
    home_val_change[rank_cols],
    unemploy_change[rank_cols]
], ignore_index=True)

combined_ranks = combined_ranks.dropna()
avg_ranks = (
    combined_ranks.groupby(['year', 'GEOID'], as_index=False)['rank']
    .mean()
    .rename(columns={'rank': 'avg_impact_rank'})
)
avg_ranks['weather_risk_rank'] = avg_ranks.groupby('year')['avg_impact_rank'].rank(
    method='dense', ascending=True
)
print(avg_ranks.head())


export_df = df.merge(avg_ranks, how='left', on=['GEOID', 'year'])
export_df['weather_risk_rank'] = np.where(
    export_df['weather_risk_rank'] <= 100,
    export_df['weather_risk_rank'],
    np.nan
)

# Test Logic before Export:
for year in export_df['year'].unique():
    for month in export_df['month_no'].unique():
        subset = export_df.query('year == @year and month_no == @month')
        n_has_rank = np.sum(~subset['weather_risk_rank'].isnull())
        if n_has_rank > 125:
            print(year, month, n_has_rank)

# Save for D3
all_years = export_df['year'].unique()
for year in all_years:
    annual_data = export_df.query('year == @year')
    annual_data.to_csv(f'../data/resilience_{year}.csv', index=False)
    print("Exported", year, "successfully")


#
# ## EVALUATE INDEX
#

# Preprocess Market Heat Index
storms_MHI = df.loc[df['major_storm_damage'], ["GEOID", "date", "Market Heat Index"]].copy()
storms_MHI = storms_MHI.rename(columns={"Market Heat Index": 'MHI'})

df_future_MHI = df[['GEOID', 'date', "Market Heat Index"]].copy()
df_future_MHI['date'] = df_future_MHI['date'] - pd.DateOffset(months=12)  # shift backward
df_future_MHI = df_future_MHI.rename(columns={"Market Heat Index": 'MHI_after_12m'})

MHI_change = pd.merge(
    storms_MHI,
    df_future_MHI,
    on=['GEOID', 'date'],
    how='left'
)

MHI_change['year'] = MHI_change['date'].dt.year
MHI_change['MHI_change'] = abs(MHI_change['MHI_after_12m'] - MHI_change["MHI"])
MHI_change['MHI_pct_change'] = MHI_change['MHI_change'] / MHI_change["MHI"]

MHI_change_yr = (
    MHI_change.groupby(["year", "GEOID"], as_index=False)["MHI_change"]
    .mean()
)

# Filter avg_ranks to top 25 per year
top25 = avg_ranks.sort_values(['year', 'avg_impact_rank']).groupby("year").head(25)

# Merge with Avg Ranks
MHI_avgranks = MHI_change_yr.merge(
    top25[["year", "GEOID", "avg_impact_rank"]],
    on=["year", "GEOID"],
    how="inner")
print(MHI_avgranks)

# Regress MHI_change on avg_rank
model = smf.ols(f'MHI_change~ avg_impact_rank', data=MHI_avgranks).fit()
print(model.summary())
