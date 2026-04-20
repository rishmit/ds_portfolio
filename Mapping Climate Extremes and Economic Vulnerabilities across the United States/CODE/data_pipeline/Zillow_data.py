import pandas as pd
import numpy as np
import os
from functools import reduce
import re

folder=r"..\data\Zillow"

files={}
for file in os.listdir(folder): 
    path=os.path.join(folder,file)
    name=os.path.splitext(file)[0]
    files[name]=pd.read_csv(path)

# print(files.items())
# print(files.keys())

Market_Heat_Index= files["Market_Heat_Index"]
New_Homeowner_Income_Needed= files["New_Homeowner_Income_Needed"]
New_Renter_Income_Needed=files["New_Renter_Income_Needed"]
ZHVI_HomeValues=files["ZHVI_HomeValues"]
ZORDI_RenterDemandIndex= files["ZORDI_RenterDemandIndex"]
ZORI_market_rate_rent= files["ZORI_market_rate_rent"]

# for df, value in files.items():
#     value["Source"]=df



#Find Date Columns and Melt dfs
def find_date_columns (df):
    date_columns=[]
    for column in df.columns:
        if re.match(r'\d{1,2}/\d{1,2}/\d{4}$',column):
            date_columns.append(column)
   
    return date_columns



def melt_dfs (df, Value=str):
    date_columns=find_date_columns(df)
    other=[c for c in df.columns if c not in date_columns]
    long=df.melt(id_vars= other,
                 value_vars = date_columns,
                 var_name = "Date", 
                 value_name = Value
                 )
    return long
    
Market_Heat_Index=melt_dfs(Market_Heat_Index, "Market Heat Index")
New_Homeowner_Income_Needed=melt_dfs(New_Homeowner_Income_Needed, "New_Homeowner_Income_Needed")
New_Renter_Income_Needed=melt_dfs(New_Renter_Income_Needed, "New_Renter_Income_Needed")
ZHVI_HomeValues=melt_dfs(ZHVI_HomeValues, "ZHVI_HomeValues")
ZORDI_RenterDemandIndex=melt_dfs(ZORDI_RenterDemandIndex, "ZORDI_RenterDemandIndex")
ZORI_market_rate_rent=melt_dfs(ZORI_market_rate_rent, "ZORI_market_rate_rent")



#Change state abbreviation to state name. Is there a built in python for this?


dfs_tomerge=[Market_Heat_Index, New_Homeowner_Income_Needed, New_Renter_Income_Needed, 
             ZHVI_HomeValues, ZORDI_RenterDemandIndex, ZORI_market_rate_rent]

# print(Market_Heat_Index.head())
# file_path=folder=r"C:\Users\katie\Desktop\GATech\GroupProject\Market_Heat_Index_test.csv"
# Market_Heat_Index.to_csv(file_path, index= False)

merged=reduce(lambda left,right: 
              pd.merge(left, right, on = list(set(left.columns) & set(right.columns)), how = "outer"),
              dfs_tomerge)

# display(merged.columns.to_list())
# columns=sorted(merged.columns, reverse=True)
# merged=merged[columns]
merged=merged[merged["RegionType"]!= "country"]
merged["StateCodeFIPS"] = merged["StateCodeFIPS"].fillna(0)
merged["MunicipalCodeFIPS"] = merged["MunicipalCodeFIPS"].fillna(0)
merged["StateCodeFIPS"]=merged["StateCodeFIPS"].astype(int)
merged["MunicipalCodeFIPS"]=merged["MunicipalCodeFIPS"].astype(int)

merged['GEOID'] = merged['StateCodeFIPS'].astype(str).str.zfill(2) + merged['MunicipalCodeFIPS'].astype(str).str.zfill(3)
# print(merged["GEOID"].unique())

# merged["RegionName"]=merged["RegionName"].str.replace(r" county$", "", case=False, regex=True).str.strip()
merged["RegionName"]=merged["RegionName"].str.replace(r",.*$", "", regex=True).str.strip() 
merged["RegionName"]=merged["RegionName"].str.upper()



##Map State abbreviations to full names
merged=merged.rename(columns ={"StateName": "state"})

state_dict = {
    "AL": "ALABAMA",
    "AK": "ALASKA",
    "AZ": "ARIZONA",
    "AR": "ARKANSAS",
    "CA": "CALIFORNIA",
    "CO": "COLORADO",
    "CT": "CONNECTICUT",
    "DE": "DELAWARE",
    "FL": "FLORIDA",
    "GA": "GEORGIA",
    "HI": "HAWAII",
    "ID": "IDAHO",
    "IL": "ILLINOIS",
    "IN": "INDIANA",
    "IA": "IOWA",
    "KS": "KANSAS",
    "KY": "KENTUCKY",
    "LA": "LOUISIANA",
    "ME": "MAINE",
    "MD": "MARYLAND",
    "MA": "MASSACHUSETTS",
    "MI": "MICHIGAN",
    "MN": "MINNESOTA",
    "MS": "MISSISSIPPI",
    "MO": "MISSOURI",
    "MT": "MONTANA",
    "NE": "NEBRASKA",
    "NV": "NEVADA",
    "NH": "NEW HAMPSHIRE",
    "NJ": "NEW JERSEY",
    "NM": "NEW MEXICO",
    "NY": "NEW YORK",
    "NC": "NORTH CAROLINA",
    "ND": "NORTH DAKOTA",
    "OH": "OHIO",
    "OK": "OKLAHOMA",
    "OR": "OREGON",
    "PA": "PENNSYLVANIA",
    "RI": "RHODE ISLAND",
    "SC": "SOUTH CAROLINA",
    "SD": "SOUTH DAKOTA",
    "TN": "TENNESSEE",
    "TX": "TEXAS",
    "UT": "UTAH",
    "VT": "VERMONT",
    "VA": "VIRGINIA",
    "WA": "WASHINGTON",
    "WV": "WEST VIRGINIA",
    "WI": "WISCONSIN",
    "WY": "WYOMING",
    "DC": "DISTRICT OF COLUMBIA"
}

merged["state"]=merged["state"].str.upper().map(state_dict)

##Seperate out the month and year from the Date column
merged["Date"]=pd.to_datetime(merged["Date"])
merged["month_no"]=merged["Date"].dt.month
merged["month"]=merged["Date"].dt.month_name()
merged["year"]=merged["Date"].dt.year



#Pull  New_Homeowner_Income_Needed Region Name or New_Renter_Income_Needed from metro areas into relevant counties 

print(merged["RegionType"].value_counts())
print(merged.shape)



# def fill_empty (row,col):
#     if row["RegionType"] != "county":
#         return row[col]
#     if not pd.isna(row[col]):
#          return row[col]
    
#     matched_row= msa_df[
#         (msa_df["state"] == row["state"]) &
#         (msa_df["year"] == row["year"]) &
#         (msa_df["month"] == row["month"])
#     ]

#     metro=str(row.get("Metro") or "").upper()

#     if isinstance(metro, str):
#         for i, msa_row in matched_row.iterrows():
#             region_name=str(msa_row.get("RegionName") or "").upper()
#             if region_name and region_name in metro:
#                 return msa_row[col]

#     return row[col]

# # merged["New_Homeowner_Income_Needed"] = merged.apply(lambda row: fill_empty(row,"New_Homeowner_Income_Needed"), axis=1)
# mask_home = (merged["RegionType"] == "county") & merged["New_Homeowner_Income_Needed"].isna()

# merged.loc[mask_home, "New_Homeowner_Income_Needed"] = (
#     merged.loc[mask_home].apply(
#         lambda row: fill_empty(row, "New_Homeowner_Income_Needed"),
#         axis=1
#     )
# )

# # merged["New_Renter_Income_Needed"] = merged.apply(lambda row: fill_empty(row,"New_Renter_Income_Needed"), axis=1)
# mask_rent = (merged["RegionType"] == "county") & merged["New_Renter_Income_Needed"].isna()

# merged.loc[mask_rent, "New_Renter_Income_Needed"] = (
#     merged.loc[mask_rent].apply(
#         lambda row: fill_empty(row, "New_Renter_Income_Needed"),
#         axis=1
#     )
# )

merged = merged.reset_index(drop=True)
merged["row_id"] = merged.index

merged["RegionName"] = merged["RegionName"].fillna("").astype(str).str.upper()
merged["Metro"] = merged["Metro"].fillna("").astype(str).str.upper()

msa=merged[merged["RegionType"]=="msa"].copy()

pull_info_from = [
    "Market Heat Index",
    "New_Homeowner_Income_Needed",
    "New_Renter_Income_Needed",
    "ZORDI_RenterDemandIndex",
]

msa = msa[
    ["state", "year", "month", "RegionName"] + pull_info_from
]


needs_fill_mask = (
    (merged["RegionType"] == "county") &
    merged[pull_info_from].isna().any(axis=1)
)

needs_fill = merged.loc[
    needs_fill_mask,
    ["row_id", "state", "year", "month", "Metro"]
].copy()


pairs = needs_fill.merge(
    msa,
    on=["state", "year", "month"],
    how="left"
)

metro_vals = pairs["Metro"].to_numpy(dtype="U")
region_vals = pairs["RegionName"].to_numpy(dtype="U")

match_mask = (
    (metro_vals != "") &
    (region_vals != "") &
    (np.char.find(metro_vals, region_vals) >= 0)
)

pairs = pairs[match_mask]

# match_mask = pairs.apply(lambda x:x["RegionName"] in x["Metro"] if ["RegionName"] and x["Metro"] else False, axis = 1)

# pairs = pairs[match_mask]


mapping = pairs.groupby("row_id")[pull_info_from].first()

for col in pull_info_from:
    merged[col] = merged[col].fillna(mapping[col])




#Name matching and formating

merged=merged.rename(columns ={"RegionName": "cz_name"})

keep=["RegionType","GEOID","state", "cz_name", "year", "month", "month_no",
      "Market Heat Index", "New_Homeowner_Income_Needed", "New_Renter_Income_Needed",
      "ZHVI_HomeValues", "ZORDI_RenterDemandIndex", "ZORI_market_rate_rent"]

final=merged[keep]
final=final[final["year"]>= 2000]

final=final[final["RegionType"]=="county"]

# final["New_Homeowner_Income_Needed"]=round(final["New_Homeowner_Income_Needed"],2)
# final["New_Renter_Income_Needed"]=round(final["New_Renter_Income_Needed"],2)
# final["ZHVI_HomeValues"]=round(final["ZHVI_HomeValues"],2)
# final["ZORI_market_rate_rent"]=round(final["ZORI_market_rate_rent"],2)

# cols_to_format = ["New_Homeowner_Income_Needed", "New_Renter_Income_Needed", "ZHVI_HomeValues", "ZORI_market_rate_rent"]

# for col in cols_to_format:
#     final[col] = final[col].apply(
#         lambda x: f"${x:,.2f}" if pd.notna(x) else ""
#     )



file_path=folder=r"..\data\zillow_data.csv"
final.to_csv(file_path, index= False)


