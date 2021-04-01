# ============================
#### Author: Aliakbar Akbaritabar (https://github.com/akbaritabar); version: August 2020 ####
# ============================


# ============================
#### Function to help with bipartite community detection ####
# ============================
def find_bipartite_partitions(graph, gamma, seed=0):
    """find_bipartite_partitions(graph, seed, gamma)
    
    Takes an igraph graph and returns the bipartite partitions detected with the provided gamma which is the internal density of the communities passed to CPM bipartite method in leiden algorithm package by Vincent Traag (https://leidenalg.readthedocs.io/en/latest/multiplex.html#bipartite). Results are returned as a pandas dataframe which includes vertices names, cluster membership and other node attributes in graph. It also return number of clusters detected and members in each of them to decide and change gamma if needed.
    
    Parameters
    ----------
    @param graph: igraph graph that needs to be bipartite (e.g., org/paper or author/paper) and named as we use vertex names and attributes in the detaframe returned.
    @param seed: the random seed to be used in CPM method to keep results/partitions replicable.
    @param gamma: CPM method (see leidenalg for more details: https://leidenalg.readthedocs.io/en/latest/multiplex.html#bipartite) takes a gamma which is the internal density of the communities detected and uses it as a resolution parameter.
    
    Examples
    --------
    >>> number_partitions, partitions_freq_members, results_df, p_01 = net_tables.find_bipartite_partitions(graph=gg, seed=0, gamma=1.625e-07)
    
    """

    import igraph as ig
    import leidenalg as la
    import pandas as pd

    optimiser = la.Optimiser()
    # set seed to get the same communities
    la.Optimiser.set_rng_seed(self=optimiser, value=seed)

    p_01, p_0, p_1 = la.CPMVertexPartition.Bipartite(
        graph, resolution_parameter_01=gamma)

    diff = optimiser.optimise_partition_multiplex(
        [p_01, p_0, p_1], layer_weights=[1, -1, -1])

    # to see number of communities detected
    number_partitions = len(set(p_01.membership))
    # pd series of clusters with number of members in each
    partitions_freq_members = pd.Series(p_01.membership).value_counts()

    # add node attributes and cluster membership and build pandas table of results
    graph.vs['cluster'] = p_01.membership
    # for all other attributes
    all_attributes = dict()
    for node_attr in graph.vs.attributes():
        attr_list = [v[node_attr] for v in graph.vs]
        all_attributes[node_attr] = attr_list
    results_df = pd.DataFrame(all_attributes)

    return (number_partitions, partitions_freq_members, results_df, p_01)
