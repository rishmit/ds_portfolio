import requests
import pandas as pd
from io import StringIO
import re
from tqdm import tqdm

BASE_URL = "https://www2.census.gov/geo/docs/reference/bndrychange/"

# Step 1: get directory listing (it's plain text/HTML with <a> tags)
resp = requests.get(BASE_URL)
resp.raise_for_status()

# Step 2: extract all .txt filenames
files = re.findall(r'href="([^"]+\.txt)"', resp.text)
# test_files = files[0:30]
print("Found files:", files)


# Step 3: loop through each file and parse
crosswalks = []
for fname in tqdm(files, total=len(files), unit="file", desc="Loading FIPS change files"):
    file_url = BASE_URL + fname
    # print(f"Downloading {file_url}...")
    txt = requests.get(file_url).text

    # Many are space-delimited; use pandas' flexibility
    try:
        df = pd.read_csv(StringIO(txt), sep='|', engine='python', dtype=str)
    except Exception:
        # fallback: comma-delimited if format changed
        df = pd.read_csv(StringIO(txt), sep=',', engine='python', dtype=str)
    except Exception:
        # fallback: space-delimited if format changed
        df = pd.read_csv(StringIO(txt), sep=r'\s+', engine='python', dtype=str)

    df['source_file'] = fname
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
    df = df.rename(columns={
        'code': 'fips_code',
        'entity_code': 'fips_code',
        'entity_name': 'entity_name',
        'area_name_(with_parent_county_and_code)': 'entity_name'
    })
    crosswalks.append(df)

# Step 4: Combine all into one DataFrame
df_all = pd.concat(crosswalks, ignore_index=True)
df_all.to_csv('FIPS_changes.csv', index=False)

# Step 5: Extract FIPS from description
FIPS_change_df = df_all[df_all['type_of_change'].str.contains('FIPS')]
FIPS_change_df = FIPS_change_df[FIPS_change_df['fips_code'].str.len() == 5]

def extract_fips_change(fips_change_row):

    desc = fips_change_row['description_of_change']
    fips_code = fips_change_row['fips_code']
    # print(desc)

    """Extract old and new FIPS codes from a freeform boundary change description."""
    # Find all numeric codes (3–5 digits)
    old_fips, new_fips = None, None
    codes = re.findall(r'\b\d{5}\b', desc)

    # Explicit "from X to Y" patterns
    m = re.search(r'from\s+(\d{5})\s+to\s+(\d{5})', desc, re.IGNORECASE)
    if m:
        old_fips = m.group(1)
        new_fips = m.group(2)
    elif "changed from" in desc.lower() and codes:
        old_fips = codes[-1]
        new_fips = fips_code
    elif "corrected from" in desc.lower() and codes:
        old_fips = codes[-1]
        new_fips = fips_code
    elif "changed to" in desc.lower() and codes:
        old_fips = fips_code
        new_fips = codes[-1]
    elif "corrected to" in desc.lower() and codes:
        old_fips = fips_code
        new_fips = codes[-1]

    return old_fips, new_fips


clean_FIPS_change_df = pd.DataFrame()
for ix, row in FIPS_change_df.iterrows():
    old_fips, new_fips = extract_fips_change(row)
    change_date = row['effective_date']
    result_row = pd.DataFrame(
        [[old_fips, new_fips, change_date]],
        columns=['old_fips', 'new_fips', 'change_date']
    )
    clean_FIPS_change_df = pd.concat([clean_FIPS_change_df, result_row], axis=0)
    # if (old_fips == None) or (new_fips == None):
    #     print(row['description_of_change'])

# Step 6: Filter to useful changes
distinct_FIPS_change_df = clean_FIPS_change_df.drop_duplicates(subset=['old_fips', 'new_fips'])
distinct_FIPS_change_df = distinct_FIPS_change_df.query('old_fips != new_fips')

# Step 7: clean & save
distinct_FIPS_change_df = distinct_FIPS_change_df[~distinct_FIPS_change_df['old_fips'].isnull()]
distinct_FIPS_change_df = distinct_FIPS_change_df[~distinct_FIPS_change_df['new_fips'].isnull()]

distinct_FIPS_change_df['change_date'] = distinct_FIPS_change_df['change_date'].str.replace('-', '/')
distinct_FIPS_change_df['change_date'] = pd.to_datetime(distinct_FIPS_change_df['change_date'], errors='coerce')

distinct_FIPS_change_df.to_csv('FIPS_delta_map.csv', index=False)
print(f"✅ Saved {len(distinct_FIPS_change_df)} mappings to FIPS_delta_map.csv")

print(distinct_FIPS_change_df.head())
