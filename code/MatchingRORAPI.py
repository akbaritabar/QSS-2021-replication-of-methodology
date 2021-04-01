# ============================
#### Author: Aliakbar Akbaritabar (https://github.com/akbaritabar); version: August 2020 ####
# ============================


import requests
import json
import pandas as pd
import time
import dask.dataframe as dd
# for URL encoding
# it is better to encode the organization addresses before sending to API (some Scopus addresses had # and & in the beginning) using: urllib.parse.quote(org_name)
import urllib.parse
from dask.distributed import Client, as_completed
client = Client()
# You can have a look at dask dashboard which shows progress of parallel tasks by un-commenting the following line (optional)
# client

# function to query local instance of ROR API docker


def ror_api_query(org_name):
    # you can either send requests to ROR online api, or set up a local version (see: https://github.com/ror-community/ror-api)
    api_url = 'http://LOCALHOST_URL:9292/organizations?affiliation='
    response = requests.get(api_url+str(urllib.parse.quote(org_name)))
    if response.ok:
        if json.loads(response.content)['number_of_results'] > 0:
            if json.loads(response.content)['items'][0]['chosen']:
                return json.loads(response.content)['items'][0]
            else:
                return 'there is no reliable elements!'
        else:
            return 'no element'
    else:
        return 'nope'

# ============================
#### Read & preprocess Scopus data ####
# ============================

berlin_scp = pd.read_csv("Berlin_scopus.csv")
# convert to dask data frame
berlin_scp_dd = dd.from_pandas(
    berlin_scp, npartitions=8).repartition(partition_size="100MB")

# write to parquet
berlin_scp_dd.to_parquet(
    '\\scp\\', engine='pyarrow', compression='snappy')
# read Berlin org-paper ties files (Scopus)
berlin_scp = dd.read_parquet(
    '\\scp\\', engine='pyarrow', compression='snappy')

# ============================
#### Parallelized ROR API requests (and disambiguation) ####
# ============================
# to time the process
t1 = time.time()
# applying above function in parallel on dask dataframe column
berlin_scp = berlin_scp.assign(ror_match=berlin_scp.loc[:, 'ADDRESS_FULL'].apply(
    ror_api_query, meta=('ADDRESS_FULL', 'str')
).compute())

# messages on process
print('#### Parallel ROR match procedure finished! ####')
t = time.time()-t1
print("Match took this much time:", t)
