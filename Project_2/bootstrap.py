'''

This is the bootstrap script to be used with
our MapReduce Jobs

We are given edges as a list of

edge_source_id   edge_sink_id   random_float

We need to select all the edges that have
REJECT_MIN <= random_float <= REJECT_MAX

and output them in a file like:

node_id [list of all nodes that link to this node]
'''

edges_filename = 'edges.txt'
out_filename = 'out.txt'
#Using NetID wjk56
#modified to make a smaller testing set
#from_net_id = 0.65;
#reject_min = 0.99 * from_net_id;
#reject_max = reject_min + 0.01;
reject_min= .1
reject_max= .8

def accept(random_float):
    return not (reject_min <= random_float <= reject_max)


out_links = {}
nodes = set()

for line in open(edges_filename, 'r'):
    source, sink, random_float = line.split()
    source = int(source)
    sink = int(sink)
    random_float = float(random_float)

    if accept(random_float):
        source_out_links = out_links.get(source, list())
        source_out_links.append(sink)
        out_links[source] = source_out_links

        # This is probably overkill, but this ensures that the corner
        # cases of a node that doesn't have any in links or a node that
        # doesn't have any out links still get added to the list of nodes without
        # me having to think too hard about the reasoning
        nodes.add(source)
        nodes.add(sink)

out_file = open(out_filename, 'w')

'''
Each line of the output is:

node_id pageranks w1 w2 .... wn
where w1 ... wn are the nodes that this node links to

EX:

0 0.54321 4 1 2 3 4
Node id = 0
Page rank for 0 = .54321
Nodes 0 links to = 1, 2, 3, 4


Note that for this bootstrap the page rank of every page will be
1/total number of pages
'''

initial_pagerank = 1.0 / len(nodes)
for source, sinks in out_links.iteritems():
    line = str(source) + " "
    line+= "%0.10f " % initial_pagerank

    for sink in sinks:
        line+= str(sink) + " "

    out_file.write(line.strip() + "\n")

all_nodes_string = ""
for node in nodes:
    all_nodes_string+= str(node) + " "

# Now we need to check if there are any dangling pages.
# Dangling pages would not be a key in the out_links dictionary
# since by definition they have no out links. So we need
# to run through our set of all nodes and check to see if they
# are a dangling node by checking if they are a key in the out_links dict

dangling_nodes = (node for node in nodes if node not in out_links)
dangling_nodes = []

for dangling_node in dangling_nodes:
    print dangling_node
    line = str(dangling_node) + " "
    line+= "%0.10f " % initial_pagerank

    line+= all_nodes_string

    out_file.write(line.strip() + "\n")


