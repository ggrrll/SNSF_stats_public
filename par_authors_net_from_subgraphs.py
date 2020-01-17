
# import useful modules
import pandas as pd
import numpy as np
import re

# modules for network analysis
import itertools as itt
import multiprocessing
import networkx as nx


# custom function
def my_graph_parallel(chunk_df):
    
    
    # create the main graph 		
    my_subgraph = nx.Graph()	

    for row in chunk_df.itertuples():
        if type(row.Authors)==str:

            partial_list = row.Authors.split(',')

            # clean strings from empty spaces
            partial_list = [p.strip() for p in partial_list]

            # create fully connected sub-graph for each publication        
            sub_graph = nx.complete_graph(len(partial_list))
            nx.relabel_nodes(sub_graph,dict(enumerate(partial_list)),copy=False)
            
            # add new graph to main one         
            my_subgraph = nx.compose(my_subgraph,sub_graph)


    return my_subgraph	


# get and clean dataset
publications = pd.read_csv('./data_SNSF/P3_PublicationExport.csv',sep=';')


# define the pool
pool_size  = multiprocessing.cpu_count()
pool = multiprocessing.Pool(pool_size)
print("we are using, max N threads on cpu =",pool_size) 


# calculate the chunk size as an integer
chunk_size = int(publications.shape[0] / pool_size)

# split original df in chuncks 
chunks = [publications.ix[publications.index[i:i + chunk_size]] for i in range(0, publications.shape[0], chunk_size)]


# run the parallel loop!
subgraph_list =  pool.map(my_graph_parallel,chunks)

# make new empty graph
authors_graph = nx.Graph()	
# merge n = pool_size subgraphs
for g in subgraph_list:
	authors_graph = nx.compose(authors_graph,g)



# save the final GRAPH!
nx.write_gpickle(authors_graph,'./authors_graph')

# on IC cluster, with 48 threads (24 cpus) took - 5/5/2017:
# real    5m56.968s
# user    96m32.334s
# sys 1m30.432s


