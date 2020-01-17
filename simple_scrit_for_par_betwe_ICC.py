import pickle
import networkx as nx


# Load the graph
with open('authors_graph', 'rb') as my_f:

    authors_graph = pickle.load(my_f, encoding='latin1')

# get connected components 
connected_comp = list(nx.connected_component_subgraphs(authors_graph))

# compute betwwenness 
import parallel_betweenness

authors_bet = parallel_betweenness.betweenness_centrality_parallel(connected_comp[0])

# save the betwwenness
pickle.dump( authors_bet, open( "authors_bet.p", "wb" ) )




