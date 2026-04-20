# Libraries
import pandas as pd

# Bring in the FEMA Public Assistance (PA) Funded Projects
pa = pd.read_csv("PublicAssistanceFundedProjectsDetails.csv", low_memory = False)

print(f"Sample Public Assistance Funding Details")
display(pa.sample(5))

# Select relevant columns
pa = pa[
    [
        'disasterNumber',
        'declarationDate',
        'stateAbbreviation',
        'stateNumberCode',
        'county',
        'countyCode',
        'incidentType',
        'projectStatus',
        'projectProcessStep',
        'projectSize',
        'projectAmount',
        'federalShareObligated',
        'totalObligated'
    ]
]

# Select dates that are after 2000-01-01
pa['declarationDate'] = pd.to_datetime(pa['declarationDate']).dt.strftime('%Y-%m-%d')
pa = pa.loc[(pa['declarationDate'] > '2000-01-01') & (pa['countyCode'] != 0) & (pa['county'] != 'Statewide') & (pa['stateNumberCode'] != 0)]

# GroupBy
pa_funds_total = pa.groupby(['disasterNumber', 'declarationDate', 'incidentType', 'stateNumberCode', 'countyCode'], as_index = False).agg({
    'projectAmount' : 'sum',
    'federalShareObligated' : 'sum',
    'totalObligated' : 'sum'
})

# Change type to int to round off
pa_funds_total[['projectAmount',
                'federalShareObligated',
                'totalObligated']] = pa_funds_total[
    ['projectAmount',
    'federalShareObligated',
    'totalObligated']
].astype(int)

# Rename cols
pa_funds_total = pa_funds_total.rename(columns = {
    'projectAmount' : 'totalProjectAmount',
    'federalShareObligated' : 'totalFedralShareObligated'
})

# Concat countyFIPS
pa_funds_total['countyFIPS'] = pa_funds_total['stateNumberCode'].astype(str).str.zfill(2) + pa_funds_total['countyCode'].astype(str).str.zfill(3)

# Change countyFIPS to int
pa_funds_total['countyFIPS'] = pa_funds_total['countyFIPS'].astype(int)

# Extract disaster month and year
pa_funds_total['declarationYear'] = pd.to_datetime(pa_funds_total['declarationDate']).dt.year
pa_funds_total['declarationMonth'] = pd.to_datetime(pa_funds_total['declarationDate']).dt.month_name()
pa_funds_total

# Get unique disasters
uniqueDisaster = pa_funds_total['incidentType'].unique()
uniqueDisaster

# save file
pa_funds_total.to_csv('pa_funds_total.csv', index = False)
