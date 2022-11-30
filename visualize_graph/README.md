# Visualize Graph
This folder contains all of the functions for visualizing Edge Graphs. 

## Network Graph
The Network Graph class takes in an Edge Graph as input and converts the graph for visualization.
This file uses NetworkX to create a network graph of the nodes in the Edge Graph.  This NetworkX
graph is then converted to a PyVis graph for visualization. This conversion is done automatically when the
Network Graph is created.  User will only need to access save_graph to access the graph after the Network 
Graph object is initialized.

### save_graph
A function that saves the Network Graph visual as an HTML  file.  This function takes in the name 
of the output HTML file as an argument. Only HTML file names are acceptable.  The file will be saved 
to the root directory. 