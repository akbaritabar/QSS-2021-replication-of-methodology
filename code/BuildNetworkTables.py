# ============================
#### Author: Aliakbar Akbaritabar (https://github.com/akbaritabar); version: August 2020 ####
# ============================


# ============================
#### Function to help with bipartite network construction ####
# ============================
def build_net_tables_graphs(raw_edges_table, orgs_cl='INSTITUTION_ID', pubs_cl='PUB_ID', orgs_attr_list=None, pubs_attr_list=None, b1type='org', b2type='paper'):
    """build_net_tables(raw_edges_table, orgs_cl='INSTITUTION_ID', pubs_cl= 'PUB_ID', orgs_attr_list=None, pubs_attr_list=None, b1type='org', b2type='paper')
    
    Takes a pandas dataframe of edges as input and builds 4 tables needed for network and reporting, i.e., edges_table, vertices_table, orgs_table and papers_table and returns igraph graphs plus the giant component of it.
    
    Parameters
    ----------
    @param raw_edges_table: Pandas dataframe with edges and node and edge attributes.
    @param orgs_cl: Column defining organization (or authors) IDs (can be duplicated). Default to 'INSTITUTION_ID' from our databases.
    @param pubs_cl: Column defining publication IDs (can be duplicated). Default to 'PUB_ID' from our databases.
    @param orgs_attr_list: List with name of columns including org (or author) attributes to appear in vertices_table and while building network as node_attributes.
    @param pubs_attr_list: List with name of columns including paper attributes to appear in vertices_table and while building network as node_attributes.
    @param b1type: Name to be used for first type of bipartite nodes (e.g., org, or author) which will be used to make unique IDs to be used in graph as node names.
    @param b2type: Name to be used for second type of bipartite nodes (e.g., paper) which will be used to make unique IDs to be used in graph as node names.
    
    Examples
  --------
  >>> edges_table, vertices_table, orgs_table, papers_table, B, Giant_comp_B, gg, Giant_comp_gg = net_tables.build_net_tables_graphs(raw_edges_table=berlin_scp, orgs_cl='org_id', orgs_attr_list=['org_id', 'org_name'], pubs_attr_list=['PUB_ID', 'YEAR', 'DOCTYPE'])
    
    """

    import pandas as pd
    import igraph as ig

    freq_total_pp = raw_edges_table.groupby(orgs_cl)[pubs_cl].nunique(
    ).reset_index().rename(columns={pubs_cl: "total_pp"}).set_index(orgs_cl)

    first_pp = raw_edges_table.groupby(orgs_cl)['YEAR'].min().reset_index(
    ).rename(columns={"YEAR": "first_pp"}).set_index(orgs_cl)
    
    last_pp = raw_edges_table.groupby(orgs_cl)['YEAR'].max().reset_index(
    ).rename(columns={"YEAR": "last_pp"}).set_index(orgs_cl)

    first_last_total_pub = pd.concat(
        [freq_total_pp, first_pp, last_pp], axis=1)

    # papers table
    papers_table = raw_edges_table.drop_duplicates(subset=pubs_cl)[
        pubs_attr_list]
    papers_table['type2'] = b2type
    papers_table['type'] = False
    papers_table['unique_id'] = [b2type+'_' +
                                 str(ss) for ss in range(1, len(papers_table)+1)]
    papers_table['name'] = papers_table['unique_id']

    # organizations/authors table
    orgs_table = raw_edges_table.drop_duplicates(subset=orgs_cl)[
        orgs_attr_list]
    orgs_table['type2'] = b1type
    orgs_table['type'] = True
    orgs_table['unique_id'] = [b1type+'_' +
                               str(ss) for ss in range(1, len(orgs_table)+1)]
    orgs_table['name'] = orgs_table['unique_id']
    # add first-last-total pub to authors
    orgs_table = orgs_table.merge(
        first_last_total_pub, left_on=orgs_cl, right_index=True, how='left')

    # vertices table
    vertices_table = pd.concat([papers_table, orgs_table], axis=0, sort=True)

    # edges table
    edges_table = raw_edges_table[[pubs_cl, orgs_cl]].drop_duplicates(subset=[
                                                                      pubs_cl, orgs_cl])
    edges_table = edges_table.merge(orgs_table, left_on=orgs_cl, right_on=orgs_cl, how='left').rename(
        columns={"unique_id": "source"})[['source', pubs_cl, orgs_cl]]
    edges_table = edges_table.merge(papers_table, left_on=pubs_cl, right_on=pubs_cl, how='left').rename(
        columns={"unique_id": "target"})[['source', 'target', 'YEAR', pubs_cl, orgs_cl]]

    # IGRAPH graph from edgelist
    # with dictionaries
    gg = ig.Graph.DictList(
        vertices=vertices_table.to_dict('records'),
        edges=edges_table.to_dict('records'),
        directed=False,
        vertex_name_attr="name",
        edge_foreign_keys=('source', 'target'))

    # take giant component
    gg_components = gg.clusters()
    Giant_comp_gg = gg_components.giant()

    return (edges_table, vertices_table, orgs_table, papers_table, gg, Giant_comp_gg)
