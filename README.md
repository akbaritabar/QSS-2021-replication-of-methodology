# QSS-2021-replication-of-methodology
Scripts needed to replicate the methodology used in "A Quantitative View of the Structure of Institutional Scientific Collaborations Using the Example of Berlin" to appear in Quantitative Science Studies.

## Preprint
You can read a preprint of the manuscript here:

Berlin: A Quantitative View of the Structure of Institutional Scientific Collaborations
Aliakbar Akbaritabar
Link: https://arxiv.org/abs/2008.08355

*Abstract*

This paper examines the structure of scientific collaborations in a large European metropolitan area. It aims to identify strategic coalitions among organizations in Berlin as a specific case with high institutional and sectoral diversity. By adopting a global, regional and organization based approach we provide a quantitative, exploratory and macro view of this diversity. We use publications data with at least one organization located in Berlin from 1996-2017. We further investigate four members of the Berlin University Alliance (BUA) through their self-represented research profiles comparing it with empirical results of OECD disciplines. Using a bipartite network modeling framework, we are able to move beyond the uncontested trend towards team science and increasing internationalization. Our results show that BUA members shape the structure of scientific collaborations in the region. However, they are not collaborating cohesively in all disciplines. Larger divides exist in some disciplines e.g., Agricultural Sciences and Humanities. Only Medical and Health Sciences have cohesive intraregional collaborations which signals the success of regional cooperation established in 2003. We explain possible underlying factors shaping the observed trends and sectoral and intra-regional groupings. A major methodological contribution of this paper is evaluating coverage and accuracy of different organization name disambiguation techniques.

## Description of scripts
Inside the `code` folder, there are six Python 3 scripts. Three are presenting the organization name disambiguation techniques as presented in the figure 1 of the manuscript (and preprint linked above) which are `OrgNameStringMatching.py`, `OrgNameFuzzyMatching.py` and `MatchingRORAPI.py`. `BipartiteCommunityDetectionDo.py` is the main module where bipartite networks are constructed and community detection happens (further detail to be found in manuscript). This module imports and uses two modules (functions), one builds the networks (`BuildNetworkTables.py`) and one is a helper function for bipartite community detection (`BipartiteCommunityDetectionHelper.py`).
