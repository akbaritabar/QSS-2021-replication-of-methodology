# ============================
#### Author: Aliakbar Akbaritabar (https://github.com/akbaritabar); version: August 2020 ####
# ============================


import importlib
import pandas as pd

# ============================
#### Read & preprocess Scopus data ####
# ============================
berlin_scp = pd.read_csv("Berlin_scopus.csv")

# ============================
#### Modules for network construction/analysis ####
# ============================
net_tables = importlib.import_module(r"BuildNetworkTables")
clusters = importlib.import_module(r"BipartiteCommunityDetectionHelper")
# In case to reload an already imported (but recently modified) module: importlib.reload(net_tables)

# build networks
edges_table, vertices_table, orgs_table, papers_table, gg, Giant = net_tables.build_net_tables_graphs(
    raw_edges_table=berlin_scp, orgs_cl='disamb_id', orgs_attr_list=['org_id', 'org_name'], pubs_attr_list=['pub_ID', 'YEAR'])

# ============================
#### Bipartite Community detection ####
# ============================

number_partitions, partitions_freq_members, results_df, p_01 = clusters.find_bipartite_partitions(
    graph=Giant, gamma=6e-03)
