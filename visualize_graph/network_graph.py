from core.computation_graph import EdgeGraph
from pyvis.network import Network
import networkx as nx

class Network_Graph(EdgeGraph):
    """An object containing the visualization of a Graph.

    A Network Graph is created from an OPM graph using NetworkX and PyVis.
    The network for the input graph is created when the object is initalized
    so the user only needs to use show_graph() to see the visualization.

    Attributes:
        nx_graph: NetworkX Object representing the input graph.
        pyvis_graph: PyVis Object representing the input graph."""

    def __init__(self, input_graph: EdgeGraph):
        self.nx_graph = nx.DiGraph()
        self.pyvis_graph = Network(directed=True)
        self.convert_graph(input_graph)

    def convert_graph(self, input_graph):
        """Creates a NetworkX and PyVis graph from the input graph.

        Using the edge in the input graph, a NetworkX graph is first created.
        This NetworkX graph is then used to create a PyVis graph
        that can be visualized.

        Args:
            input_graph: EdgeGraph to convert."""

        for role, parent, child in input_graph.edges:
            #Add both nodes to the graph
            if not self.nx_graph.has_node(parent.name):
                #TODO: when roles integrated, add group=role as argument
                #unsure how to address node belonging to multiple group, can potentall
                #use title to list all roles of this node
                self.nx_graph.add_node(parent.name)
            if not self.nx_graph.has_node(child.name):
                self.nx_graph.add_node(child.name)

            #add directed edge to the graph
            self.nx_graph.add_edge(parent.name, child.name)

        #convert nx_graph to pyvis graph
        self.pyvis_graph.from_nx(self.nx_graph)

    def save_graph(self, file_name):
        """Saves the graph as an HTML file for viewing.

        File name must be an HTML.

        Args:
            file_name: Name of HTML file to save graph to."""

        #this options helps to keep that graph in a straight line
        self.pyvis_graph.set_options("""{ "physics": {"barnesHut": {"centralGravity": 0}}}""")
        self.pyvis_graph.show(file_name)