# MapReduce-Grouping-Nodes-based-on-number-of-neighbors
### This project implements a simple graph algorithm that needs two Map-Reduce jobs. A directed graph is represented as a text file where each line represents a graph edge. 

### For example,
10,20
represents the directed edge from node 10 to node 20. 

### First, for each graph node, we compute the number of node neighbors. 
### Then, we group the nodes by their number of neighbors and for each group we count how many nodes belong to this group. 

### The result will have lines such as:
11 31
which says that there are 31 nodes that have 11 neighbors.
