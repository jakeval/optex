from core.computation_graph import EdgeGraph, Process
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

    def __init__(
        self, input_graph: EdgeGraph, notebook=False, show_agents: bool = True
    ):
        self.show_agents = show_agents
        self.nx_graph = nx.DiGraph()
        self.pyvis_graph = Network(directed=True, notebook=notebook)
        self.convert_graph(input_graph)
        self.name = input_graph.name

    def convert_graph(self, input_graph: EdgeGraph):
        """Creates a NetworkX and PyVis graph from the input graph.

        Using the edge in the input graph, a NetworkX graph is first created.
        This NetworkX graph is then used to create a PyVis graph
        that can be visualized.

        Args:
            input_graph: EdgeGraph to convert."""

        node_to_int = {}
        next_id = 0
        for role, parent, child in input_graph.edges:
            # Add both nodes to the graph
            if parent in node_to_int:
                pid = node_to_int[parent]
            else:
                pid = next_id
                node_to_int[parent] = pid
                next_id += 1

            if child in node_to_int:
                cid = node_to_int[child]
            else:
                cid = next_id
                node_to_int[child] = cid
                next_id += 1

            if not self.nx_graph.has_node(pid):
                # TODO: when roles integrated, add group=role as argument
                # unsure how to address node belonging to multiple group, can potentall
                # use title to list all roles of this node
                shape = "dot"
                if isinstance(parent, Process):
                    shape = "square"
                label = parent.name
                if parent.agents and self.show_agents:
                    label += f" ({parent.agents})"
                self.nx_graph.add_node(pid, label=label, shape=shape)
            if not self.nx_graph.has_node(cid):
                shape = "dot"
                if isinstance(child, Process):
                    shape = "square"
                label = child.name
                if child.agents and self.show_agents:
                    label += f" ({child.agents})"
                self.nx_graph.add_node(cid, label=label, shape=shape)

            # add directed edge to the graph
            self.nx_graph.add_edge(pid, cid, label=role)

        # convert nx_graph to pyvis graph
        self.pyvis_graph.from_nx(self.nx_graph)

        # ellipse, circle, database, box, text.
        # image, circularImage, diamond, dot, star, triangle, triangleDown, square and icon.

    def save_graph(self, file_name=None):
        """Saves the graph as an HTML file for viewing.

        File name must be an HTML.

        Args:
            file_name: Name of HTML file to save graph to."""

        # this options helps to keep that graph in a straight line
        self.pyvis_graph.set_options(
            """{ "physics": {"barnesHut": {"centralGravity": 0}}}"""
        )

        filename = file_name or f"{self.name}.html"

        return self.pyvis_graph.show(filename)
