import statsmodels as sm
import statsmodels.formula.api as smf
import pandas as pd
import numpy as np



#calculate baseline_y= mean or median outcome value over the previous month(t; month resiliance index was calculated on)
#Calculate y (t+1), y(t+3), y(t+6)
#predict outcome variables at t + one month, 3 months, and 12 months to assess accuracy of index in prediciting outcomes over time
# y (t+1), y(t+3), y(t+6), y(t+12)   ~ resiliance_index + baseline_y

resilience_data = pd.read_csv('../data/resilience.csv')
print(resilience_data.columns)

#This should already be grouped by GEOID and NAs filled in
#I think ZHVI_HomeValues needs to be adjusted for inflation
#Unsure about Unemployment Rate
X=resilience_data[['resilience_index']]
# y=resilience_data[["Market Heat Index", "ZHVI_HomeValues", "Unemployment Rate"]]
y=resilience_data[[ "Unemployment Rate"]]
# c= smf.add_constant(X)

#Calculate rolling medians for each outcome 

for column in y.columns:
    baseline=f"baseline_{column}"
    resilience_data[baseline] = (
    resilience_data.groupby('GEOID')[column] \
      .rolling(window=12, min_periods=1)
      .median()
      .reset_index(level=0, drop=True)
)

##Need to add the +1, +3, +6, +12

month_offset=[1,3,6,12]

for column in y.columns:
    for m in month_offset:
        outcome_offset=f"{column}_+{m}"
        resilience_data[outcome_offset]= (
            resilience_data.groupby("GEOID")[column] \
            .shift(-m)
        )git reset --hard origin/main


def regress(outcome, time, df):
    y= f"{outcome}_+{time}"
    base=f"baseline_{outcome}"
    model =smf.ols(f'Q("{y}") ~ Q("resilience_index") + Q("{base}")', data = df).fit()
    return model
    
# validate_MHI=regress("Market Heat Index", 1,  resilience_data)
# print(validate_MHI.summary())

# validate_MHI=regress("ZHVI_HomeValues", 1,  resilience_data)
# print(validate_MHI.summary())

validate_MHI=regress("Unemployment Rate", 1,  resilience_data)
print(validate_MHI.summary())

validate_MHI=regress("Unemployment Rate", 3,  resilience_data)
print(validate_MHI.summary())


#Plot regression lines
# Cross validate if we have time



