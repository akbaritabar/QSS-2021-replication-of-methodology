# ============================
#### Author: Aliakbar Akbaritabar (https://github.com/akbaritabar); version: August 2020 ####
# ============================

# ============================
#### Steps in brief ####
# ============================
# 1- Read your input data as pandas data frames, normalize organization names (described in methods of articles)
# 2- Convert them to Dask (http://dask.org/) data frame format for parallelisation
# 3- Write to parquet format (a column based format instead of csv which is row based) and is optimized for parallelisation
# 4- Read the parquet files, do the parallelized fuzzy matching as shown below

import time
from dask.distributed import Client, progress
from fuzzywuzzy import process, fuzz
import pandas as pd
import dask.dataframe as dd

# ============================
#### Read & preprocess Scopus & GRID data ####
# ============================

berlin_scp = pd.read_csv("Berlin_scopus.csv")
# convert to dask data frame
berlin_scp_dd = dd.from_pandas(
    berlin_scp, npartitions=8).repartition(partition_size="100MB")

# read Grid data
grid_data = pd.read_csv("grid_data.csv")
# convert to dask data frame
grid_data_dd = dd.from_pandas(
    grid_data, npartitions=8).repartition(partition_size="100MB")

# ============================
#### Normalize organization names ####
# ============================
# Now you need to normalize organization names in both sides of the matching data, based on the steps described in methods section of the article to then use it in the following.

# write to parquet
berlin_scp_dd.to_parquet(
    '\\scp\\', engine='pyarrow', compression='snappy')

grid_data_dd.to_parquet(
    '\\grid\\', engine='pyarrow', compression='snappy')

# read parquet formats
berlin_scp_dd = dd.read_parquet(
    '\\scp\\', engine='pyarrow', compression='snappy')

grid_data_dd = dd.read_parquet(
    '\\scp\\', engine='pyarrow', compression='snappy')

# ============================
#### Fuzzy matching in parallel ####
# ============================

# helper function for fuzzy matching

def fuzzy_match(entity, choices, scorer, cutoff):
    return process.extractOne(
        entity, choices=choices, scorer=scorer, score_cutoff=cutoff
    )

# parallelized fuzzy matching process

client = Client(n_workers=32, threads_per_worker=16, memory_limit='10GB')
# You can have a look at dask dashboard which shows progress of parallel tasks by un-commenting the following line (optional)
# client

# to time the process
t1 = time.time()
# parallel fuzzy match
berlin_scp_dd = berlin_scp_dd.assign(matching_results=berlin_scp_dd.loc[:, 'normalized_name'].apply(
    fuzzy_match,
    args=(
        grid_data_dd.result().loc[:, 'normalized_name'],  # choices
        fuzz.ratio,                              # scorer
        80                                       # cutoff
    ), meta=('normalized_name', 'str')
).compute())

# messages on process
print('#### Parallel match procedure finished! ####')
t = time.time()-t1
print("Match took this much time:", t)
