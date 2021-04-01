# ============================
#### Author: Aliakbar Akbaritabar (https://github.com/akbaritabar); version: August 2020 ####
# ============================


import pandas as pd
# ============================
#### Read & preprocess Scopus data ####
# ============================
# read organization data (in our study, it is from Scopus)
berlin_scp = pd.read_csv("Berlin_scopus.csv")

# read GRID data
grid_data = pd.read_csv("grid_data.csv")

# ============================
#### Normalize organization names ####
# ============================
# Now you need to normalize organization names in both sides of the matching data, based on the steps described in methods section of the article to then use it in the following.

# ============================
#### OrgNameStringMatching using string methods in python ####
# ============================

exact_match_detection = []
for p_in, p in enumerate(berlin_scp['normalized_name'].str.lower()):
    for r_in, r in enumerate(grid_data['normalized_name'].str.lower()):
        if p in r:
            exact_match_detection.append((p, p_in, r, r_in))

print('GRID match finished!')
