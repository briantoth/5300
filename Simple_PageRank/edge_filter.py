def filter_edges():
    #netid bdt25
    from_netid= .52
    reject_min= .99 * from_netid
    reject_limit= reject_min + .01
    out= open('modified_edges', 'w')
    with open('edges.txt', 'r') as f:
        for line in f:
            line= line.strip().split()
            rand_num= float(line[2])
            if not (rand_num >= reject_min and rand_num < reject_limit):
                out.write(line[0] + ' ' + line[1] + '\n')

    out.close()

if __name__ == '__main__':
    filter_edges()
