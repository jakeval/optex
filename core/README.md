# Core
This folder contains all of the functions and decorators for creating an optex pipeline and merging pipelines. 

## Computation Graph 
This file contains the decorators.  Since we need to create computation graphs for merging without evaluating the values of each node, there is a global _DRY_RUN variable that indicated whether or not nodes should be evaluated.  The computation graph is based on the design of the Open Provenance Model (OPM). 

### Graph, EdgeGraph, Node
Graph is a class used to store the input and output Artifacts of a computation graph. This can be created from a Process. 

EdgeGraph a Graph that stores the inputs, outputs, and edges of a computation graph. Edges are tuples with the format: role, parent node, child node.  This can be crated from a Process or output Artifacts. 

A Node can either be a Process or an Artifact as defined in the following sections. 
 

### Artifact
An Artifact is a node that stores the input or output of a Process.  This is a wrapper class.  The optex library is able to handle unwrapping these objects to access the internal data so the user can sinply pass an Artifact to a Process, they should not try to pass the data store in the Artifact. 

### Process
A Process is a function that accepts and return an Artifact. Processes must be static functions with immutable behavior.

### optex_process
optex_process should be used a decorator that converts a python function that performs computation on data contained within an Artifact into an OptexProcess. This function accepts string(s) describing the names/roles of Artifacts returned and returns a new OptexProcess wrapping the decorated function. Using this decorator will automatically construct the computation graph. The python function should accept the data that the Artifact contains.  This decorator will automatically unwrap the Artifact so the user should write the python function such that it is manipulating the internal data directly. 

### optex_composition 
optex_composition should be used as a decorator that converting python functions which contain OptexProcesses to an OptexComposition.  This function accepts string(s) describing the names/roles of Artifacts and Processes returned and returns a new OptexProcess wrapping the decorated function. Using this decorator will automatically construct the computation graph. Functions using this decorator CANNOT mutate an Artifact directly, they must all an OptexProcess that mutate the Artifact.

## Graph Merge
This file has the ability to take two computation graphs and merge the graphs based on which nodes have the same Artifacts as inputs and use the same OptexProcess. 

### Make Expanded Graph Copy
This function takes in a Graph and replaces all of the composition processes with their subgraphs.  It accepts a Graph as an input and returns a new Graph with all composition nodes expanded. This function uses the _DRY_RUN global variable in ComputationGraph to create the graph without executing any processes. 

### Execute Graph
This function executes a statically generated graph on inputs.  It accepts a Graph and Artifacts as inputs and returns a mapping from the output Artifact to the value it computes to. This function uses the _DRY_RUN variable in ComputationGraph to execute all processes. 
