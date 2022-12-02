from typing import Sequence, Tuple, Mapping, Any
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
    elif isinstance(node, computation_graph.Process):
        new_node = computation_graph.Process(node._transformation)
        new_node.returns_indices = node.returns_indices
    else:
        raise RuntimeError(
            f"{node} is not an Artifact or a Process. Did you forget to call the optex decorator?"
        )
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
    top_level_children = set(zip(artifact.children, artifact.children_roles))
    while top_level_children:
        child, edge_key = top_level_children.pop()

        if not edge_key:
            continue

        if not child.child_processes:
            composed_children.append((edge_key, child))
        else:
            child_roles = []
            child_processes = []
            for child_process in child.child_processes:
                for child_parent, child_role in zip(
                    child_process.parents, child_process.parent_roles
                ):
                    if child_parent == artifact:
                        child_roles.append(child_role)
                        child_processes.append(child_process)
                        continue
            top_level_children = top_level_children.union(
                zip(child_processes, child_roles)
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
        new_node.agents.append(graph.name)
        old_node = old_nodes[new_node]

        if isinstance(old_node, computation_graph.Artifact):
            old_children = get_composed_children(old_node)
        elif isinstance(old_node, computation_graph.Process):
            old_children = zip(old_node.children_roles, old_node.children)

        for old_key, old_child in old_children:
            if old_child in new_nodes:
                new_child = new_nodes[old_child]
            else:
                new_child = copy_node(old_child)
                new_nodes[old_child] = new_child
                old_nodes[new_child] = old_child
            new_child.parents.append(new_node)
            new_child.parent_roles.append(old_key)
            new_node.children.append(new_child)
            new_node.children_roles.append(old_key)
            open_set.add(new_child)
        explored_set.add(new_node)

    # TODO(@jakeval): Why can graph.outputs not be a list? This is a bug.
    try:
        outputs = [new_nodes[old_output] for old_output in graph.outputs]
    except TypeError:
        outputs = [new_nodes[graph.outputs]]

    return computation_graph.EdgeGraph.from_output_artifacts(
        outputs, graph.name
    )


def topological_sort_graph(
    edges: Sequence[Tuple[computation_graph.Node, computation_graph.Node]]
) -> Sequence[computation_graph.Node]:
    node_count = 0
    node_to_int = {}
    int_to_node = {}
    int_edges = []
    for _, parent_node, child_node in edges:
        if parent_node not in node_to_int:
            node_to_int[parent_node] = node_count
            int_to_node[node_count] = parent_node
            node_count += 1
        if child_node not in node_to_int:
            node_to_int[child_node] = node_count
            int_to_node[node_count] = child_node
            node_count += 1
        parent_int = node_to_int[parent_node]
        child_int = node_to_int[child_node]
        int_edges.append((parent_int, child_int))

    sorted_nodes = topologicalSort(node_count, int_edges)
    return [int_to_node[node_int] for node_int, _ in sorted_nodes]


def topologicalSort(totalVertices, prerequisites):
    ##make graph
    graph = {}
    for edge in prerequisites:
        if edge[0] not in graph:
            graph[edge[0]] = [edge[1]]
        else:
            graph[edge[0]].append(edge[1])

    n = totalVertices
    indegree = [0] * n
    answer = []
    for key in graph:
        for nbrs in graph[key]:
            indegree[nbrs] += 1
    queue = []
    for i in range(0, n):
        if indegree[i] == 0:
            queue.append((i, 0))

    while len(queue) > 0:
        rem = queue.pop(0)
        answer.append((rem[0], rem[1]))
        if rem[0] in graph:
            for child in graph.get(rem[0]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append((child, rem[1] + 1))

    if len(answer) != n:
        raise RuntimeError(
            "Graph had cycles -- topological sort is impossible."
        )

    return answer


# @TODO(@jakeval): This ignores roles
def get_merge_candidates(
    old_node: computation_graph.Node,
    old_to_new: Mapping[computation_graph.Node, computation_graph.Node],
) -> Sequence[computation_graph.Node]:
    old_parents = old_node.parents
    new_parents = [old_to_new[old_parent] for old_parent in old_parents]

    children_sets = []
    for parent in new_parents:
        children_sets.append(set(parent.children))
    if children_sets:
        return children_sets[0].intersection(*children_sets[1:])
    else:
        return []


def can_merge(
    old_node: computation_graph.Node,
    new_node: computation_graph.Node,
    old_to_new: Mapping[computation_graph.Node, computation_graph.Node],
):
    """Returns true if the original node can be merged into an already-existing
    node in the new graph.

    Args:
        old_node: The node from the original graph.
        new_node: The merge candidate in the new graph.
        old_to_new: A mapping from original graph nodes to merged graph
        nodes."""

    # Check that their types match
    if type(old_node) != type(new_node):
        return False

    # Check that process transformation functions match
    if isinstance(old_node, computation_graph.Process):
        if old_node._transformation != new_node._transformation:
            return False

    # Check that parents match
    old_to_new_parents = [
        (role, old_to_new[parent])
        for role, parent in zip(old_node.parent_roles, old_node.parents)
    ]
    if set(old_to_new_parents) == set(
        zip(new_node.parent_roles, new_node.parents)
    ):
        return True
    else:
        return False


def add_input_artifacts(inputs):
    old_to_new = {}
    for new_artifact in inputs:
        for graph_name, old_artifacts in inputs[new_artifact].items():
            for old_artifact in old_artifacts:
                new_artifact.agents.append(graph_name)
                old_to_new[old_artifact] = new_artifact

    return old_to_new


def merge_nodes(
    old_node: computation_graph.Node,
    merge_candidate: computation_graph.Node,
    old_to_new: Mapping[computation_graph.Node, computation_graph.Node],
) -> Mapping[computation_graph.Node, computation_graph.Node]:
    old_to_new[old_node] = merge_candidate
    merge_candidate.agents += old_node.agents
    return old_to_new


def add_new_node(
    old_node: computation_graph.Node,
    old_to_new: Mapping[computation_graph.Node, computation_graph.Node],
) -> Tuple[
    computation_graph.Node,
    Mapping[computation_graph.Node, computation_graph.Node],
    Mapping[computation_graph.Node, computation_graph.Node],
]:
    new_node = copy_node(old_node)
    new_node.agents = old_node.agents
    old_to_new[old_node] = new_node
    return new_node, old_to_new


def add_edges(
    old_node: computation_graph.Node,
    new_node: computation_graph.Node,
    old_to_new: Mapping[computation_graph.Node, computation_graph.Node],
) -> None:
    for role, old_parent in zip(old_node.parent_roles, old_node.parents):
        new_parent = old_to_new[old_parent]
        new_node.parents.append(new_parent)
        new_node.parent_roles.append(role)
        new_parent.children.append(new_node)
        new_parent.children_roles.append(role)


def add_nodes(
    old_nodes: Sequence[computation_graph.Node],
    old_to_new: Mapping[computation_graph.Node, computation_graph.Node],
) -> Tuple[
    Mapping[computation_graph.Node, computation_graph.Node],
    Mapping[computation_graph.Node, computation_graph.Node],
]:
    for old_node in old_nodes:
        if old_node in old_to_new:  # the node was previously added
            continue

        merge_candidates = get_merge_candidates(old_node, old_to_new)

        # Try to merge the node in to the new graph.
        did_merge = False
        for merge_candidate in merge_candidates:
            if can_merge(old_node, merge_candidate, old_to_new):
                old_to_new = merge_nodes(old_node, merge_candidate, old_to_new)
                did_merge = True
                break

        # If it can't merge, add a new node to the new graph.
        if not did_merge:
            new_node, old_to_new = add_new_node(old_node, old_to_new)
            add_edges(old_node, new_node, old_to_new)

    return old_to_new


def merge_graphs(
    graphs: Sequence[computation_graph.EdgeGraph],
    inputs: Mapping[
        computation_graph.Artifact,
        Mapping[str, Sequence[computation_graph.Artifact]],
    ],
    name: str,
):
    """ """
    graph_nodes = dict(
        [(graph.name, topological_sort_graph(graph.edges)) for graph in graphs]
    )

    inputs_copy = {}
    for node, v in inputs.items():
        node_copy = copy_node(node)
        inputs_copy[node_copy] = v

    old_to_new = add_input_artifacts(inputs_copy)

    for graph_name, nodes in graph_nodes.items():
        old_to_new = add_nodes(nodes, old_to_new)

    inputs = {}
    all_inputs = set()
    for graph in graphs:
        new_inputs = [old_to_new[input] for input in graph.inputs]
        inputs[graph.name] = new_inputs
        all_inputs = all_inputs.union(new_inputs)
    inputs["all_inputs"] = list(all_inputs)

    outputs = {}
    all_outputs = set()
    for graph in graphs:
        new_outputs = [old_to_new[output] for output in graph.outputs]
        outputs[graph.name] = new_outputs
        all_outputs = all_outputs.union(new_outputs)
    outputs["all_outputs"] = list(all_outputs)

    return (
        computation_graph.EdgeGraph.from_output_artifacts(
            list(all_outputs), name=name
        ),
        inputs,
        outputs,
    )


def get_inputs(call_list):
    """
    [(graph, transformation, role, value)]

    {
        merged_artifact: {
            'graph_name': [input_artifacts]
        }
    }
    """
    inputs = {}

    for graph, transformation, role, artifact in call_list:
        input_artifact = None
        for input in graph.inputs:
            for child_role, child in zip(input.children_roles, input.children):
                if (child._transformation == transformation.__wrapped__) and (
                    child_role == role
                ):
                    input_artifact = input
                    break
            if input_artifact is not None:
                break
        if not input_artifact:
            raise RuntimeError(
                f"Can't find the corresponding process for {transformation.__wrapped__}, {role}"
            )

        if artifact in inputs:
            if graph.name in inputs[artifact]:
                inputs[artifact][graph.name].append(input_artifact)
            else:
                inputs[artifact][graph.name] = [input_artifact]
        else:
            inputs[artifact] = {graph.name: [input_artifact]}
    return inputs


# TODO(@jakeval): Because values are cached per-artifact and not per-process,
# processes which return multiple values must be recomputed.
def compute_artifact_ancestors(artifact, artifact_values):
    """Recursively computes the value of an artifact and its ancestors given
    sufficient values for its ancestors.

    This is used to lazily compute an artifact given values for its root
    ancestors. The function will recursively evaluate all of the necessary
    ancestor values starting from the root ancestors until it computes the
    target artifact value.

    Args:
        artifact: The artifact whose value and ancestors to compute.
        artifact_values: The values of the artifact's root ancestors.

    Returns:
        The values of the artifact and all its ancestors.
    """
    # artifacts only have 1 parent
    process = artifact.parents[0]
    process_args = {}
    for arg_name, parent_artifact in zip(
        process.parent_roles, process.parents
    ):
        if parent_artifact not in artifact_values:
            artifact_values = compute_artifact_ancestors(
                parent_artifact, artifact_values
            )
        process_args[arg_name] = artifact_values[parent_artifact]
    if process.returns_indices:
        index = process.returns_indices[artifact.parent_roles[0]]
        artifact_values[artifact] = process._transformation(**process_args)[
            index
        ]
    else:
        artifact_values[artifact] = process._transformation(**process_args)
    return artifact_values


def execute_graph(
    graph: computation_graph.Graph,
    inputs: Mapping[computation_graph.Artifact, Any],
) -> Mapping[computation_graph.Artifact, Any]:
    """Executes a statically-generated computation graph on some inputs.

    graph: The graph to execute.
    inputs: A mapping from input Artifact to the value it should take on in
        the computation.

    Returns:
        A mapping from output Artifact to the value it computes to.
    """
    output_values = {}
    artifact_values = inputs.copy()

    for output in graph.outputs:
        artifact_values = compute_artifact_ancestors(output, artifact_values)
        output_values[output] = artifact_values[output]

    return output_values


def get_merged_inputs(graph_inputs, call_list):
    """
    Given:
    {
        value: (graph_name, transformation, role)
    }

    Return:
    {
        value: merged_graph_input_artifact
    }
    """
    inputs = {}

    for value, (graph_name, transformation, role) in call_list.items():
        input_artifact = None
        for input in graph_inputs[graph_name]:
            for child_role, child in zip(input.children_roles, input.children):
                if (child._transformation == transformation.__wrapped__) and (
                    child_role == role
                ):
                    input_artifact = input
                    break
            if input_artifact is not None:
                break
        if not input_artifact:
            raise RuntimeError(
                f"Can't find the corresponding process for {transformation.__wrapped__}, {role}"
            )
        inputs[input_artifact] = value

    return inputs


def execute_merged_graph(
    graph: computation_graph.Graph,
    inputs: Mapping[computation_graph.Artifact, Any],
) -> Mapping[computation_graph.Artifact, Any]:
    """Executes a statically-generated computation graph on some inputs.

    Given:

    input_artifact, value

    input_artifact:
    - graph, role,
    -


    graph: The graph to execute.
    inputs: A mapping from input Artifact to the value it should take on in
        the computation.

    Returns:
        A mapping from output Artifact to the value it computes to.
    """
    output_values = {}
    artifact_values = inputs.copy()

    for output in graph.outputs:
        artifact_values = compute_artifact_ancestors(output, artifact_values)
        output_values[output] = artifact_values[output]

    return output_values
