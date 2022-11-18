from typing import Sequence, Tuple
from core import computation_graph


# TODO(@jakeval): Remove the debugging name attribute from the Node class.


def copy_node(node: computation_graph.Node) -> computation_graph.Node:
    """Shallow copies a node without its edges.

    Args:
        node: The node to copy.

    Returns:
        A copied node identical to the original but without any edges.
    """
    if isinstance(node, computation_graph.Artifact):
        new_node = computation_graph.Artifact(node._data)
    if isinstance(node, computation_graph.Process):
        new_node = computation_graph.Process(node._transformation)
    new_node.name = node.name
    return new_node


def get_composed_children(
    artifact: computation_graph.Artifact,
) -> Sequence[Tuple[str, computation_graph.Process]]:
    """Gets the innermost children of an Artifact, even if they are composed
    within another OptexProcess.

    Normally, the children of an Artifact are the processes that directly use
    that Artifact, even if those processes are just the compositions of other
    processes that use it. This function recurses over the composed processes
    using this Artifact, eliminating all composition functions and including
    only the leaf process children.

    Args:
        artifact: The artifact whose children to find.

    Returns:
        A list of (key, child) pairs where each child is a child node and each
        key is the role connecting that child to the original Artifact.
    """
    composed_children = []
    top_level_children = set(artifact.children.values())
    while top_level_children:
        child = top_level_children.pop()
        edge_key = None
        for k, v in child.parents.items():
            if v == artifact:
                edge_key = k
                break
        if not edge_key:
            continue

        if not child.child_processes:
            composed_children.append((edge_key, child))
        else:
            top_level_children = top_level_children.union(
                child.child_processes
            )

    return composed_children


def make_expanded_graph_copy(
    graph: computation_graph.Graph,
) -> computation_graph.EdgeGraph:
    """Makes a copy of the graph where all composition processes have been
    replaced with their subgraphs.

    Args:
        graph: The graph to copy.

    Returns:
        A new graph where composition processes have been replaced by the
        subgraphs they contain.
    """
    explored_set = set()
    new_nodes = {}
    old_nodes = {}

    new_inputs: Sequence[computation_graph.Node] = []
    for old_input in graph.inputs:
        new_input = copy_node(old_input)
        new_inputs.append(new_input)
        new_nodes[old_input] = new_input
        old_nodes[new_input] = old_input

    open_set = set(new_inputs)
    while open_set:
        new_node = open_set.pop()
        if new_node in explored_set:
            continue
        old_node = old_nodes[new_node]

        if isinstance(old_node, computation_graph.Artifact):
            old_children = get_composed_children(old_node)
        elif isinstance(old_node, computation_graph.Process):
            old_children = old_node.children.items()

        for old_key, old_child in old_children:
            if old_child in new_nodes:
                new_child = new_nodes[old_child]
            else:
                new_child = copy_node(old_child)
                new_nodes[old_child] = new_child
                old_nodes[new_child] = old_child
            new_child.parents[old_key] = new_node
            new_node.children[old_key] = new_child
            open_set.add(new_child)
        explored_set.add(new_node)

    outputs = [new_nodes[old_output] for old_output in graph.outputs]
    return computation_graph.EdgeGraph.from_output_artifacts(outputs)
