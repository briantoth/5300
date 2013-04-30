'''
Script to parse the output from the last reducer
cycle and output the pagerank of the
highest numbered node in each block
'''

reducer_output_filename = 'blocked_reducer_output'

# A map from block_num -> (highest_node_num, pagerank of node)
block_map = {}

for line in open(reducer_output_filename, 'r'):
    _, node_num, block_num, pr = line.split()[:4]
    node_num = int(node_num)
    block_num = int(block_num)
    pr = float(pr)

    current_highest, _ = block_map.get(block_num, (-1, _))
    if node_num > current_highest:
        block_map[block_num] = (node_num, pr)

for block_num in sorted(block_map.keys()):
    node, pr = block_map[block_num]
    print 'Block Number ', block_num
    print '\tHighest node ', node
    print '\tPage rank ', pr