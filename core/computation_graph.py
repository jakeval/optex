from __future__ import annotations
from typing import (
    Any,
    Optional,
    Mapping,
    Callable,
    Tuple,
    Union,
    Sequence,
    Protocol,
)
from functools import wraps
import inspect
import contextlib
import dataclasses


# TODO(@jakeval): Remove the debugging name attribute from the Node class.


_DRY_RUN: bool = False
"""Used to create static graphs by replacing computation with no-ops.

Should only be changed within the scope of the dry_run_context manager.
"""


@contextlib.contextmanager
def dry_run_context() -> None:
    """Creates a context-managed scope where calling optex processes builds the
    graph but does not execute the computation.

    This is useful for generating a static graph of a computation before
    executing the actual computation.
    """
    global _DRY_RUN
    try:
        _DRY_RUN = True
        yield
    finally:
        _DRY_RUN = False


def find_roots(leafs: Sequence[Artifact]) -> Sequence[Artifact]:
    """Finds the roots of a computation graph given its leafs.

    Args:
        leafs: The leaf Artifacts generated by the computation graph.

    Returns:
        A list of the computation graph root artifacts."""
    open_set = set(leafs)
    explored_set = set()

    roots = set()

    while open_set:
        node = open_set.pop()
        if node in explored_set:
            continue
        if node.parents:
            open_set = open_set.union(node.parents.values())
        else:
            roots.add(node)
        explored_set.add(node)

    return list(roots)


def get_edge_graph(graph: Graph) -> EdgeGraph:
    """Lists a graph's edges and returns them in a shallow copy of the
    graph."""
    open_set = set(graph.inputs)
    explored_set = set()
    edges = []
    while open_set:
        node = open_set.pop()
        if node in explored_set:
            continue
        explored_set.add(node)
        for role, child in node.children.items():
            edges.append((role, node, child))
            open_set.add(child)

    return EdgeGraph(graph.inputs, graph.outputs, edges)


# TODO(@jakeval): This function should be part of the Graph class.
def generate_static_graph(
    process: OptexProcess.transform,
) -> Graph:
    """Generates a computation graph for an optex process without executing the
    computation.

    Args:
        process: The optex process to generate a graph for.

    Returns:
        A Graph object storing a list of the graph input Artifacts and output
        Artifacts.
    """
    with dry_run_context():
        expected_params = inspect.signature(process).parameters
        dummy_params = [Artifact(None) for _ in expected_params]
        outputs = process(*dummy_params)
        if isinstance(outputs, Tuple):
            roots = find_roots(outputs)
        else:
            roots = find_roots([outputs])
        return Graph(roots, outputs)


@dataclasses.dataclass
class Graph:
    """Stores the input and output Artifacts of a computation graph.

    The full graph can be traversed top-down starting from the inputs.

    Attributes:
        inputs: The top-level parentless Artifacts that are the graph root.
        outputs: The Artifacts that are returned by the top-level optex
            process containing the computation graph. These are not necessarily
            leaves of the computation graph.
    """

    inputs: Sequence[Artifact]
    outputs: Sequence[Artifact]

    @staticmethod
    def from_process(
        process: OptexProcess.transform,
    ) -> Graph:
        """Generates a computation graph for an optex process without executing
        the computation. Typically the process used is a function decorated
        with optex_composition which makes calls to many other functions
        decorated with optex_process.

        Args:
            process: The optex process to generate a graph for.

        Returns:
            A Graph object storing a list of the graph input Artifacts and
            output Artifacts.
        """
        return generate_static_graph(process)


@dataclasses.dataclass
class EdgeGraph(Graph):
    """Stores the inputs, outputs, and edges of a computation graph.

    Edges are tuples of the format (role, parent node, child node). The edge
    list does not edges from subgraphs contained in composition processes --
    composition processes replace the subgraphs they contain.

    Attributes:
        inputs: The top-level parentless Artifacts that are the graph root.
        outputs: The Artifacts that are returned by the top-level optex
            process containing the computation graph. These are not necessarily
            leaves of the computation graph.
        edges: A list of graph edges where each edge is a tuple of (role,
            parent node, child node).
    """

    edges: Sequence[str, Node, Node]

    @staticmethod
    def from_process(
        process: OptexProcess.transform,
    ) -> EdgeGraph:
        """Generates a computation graph for an optex process without executing
        the computation. Typically the process used is a function decorated
        with optex_composition which makes calls to many other functions
        decorated with optex_process.

        Args:
            process: The optex process to generate a graph for.

        Returns:
            A Graph object storing a list of the graph input Artifacts and
            output Artifacts.
        """
        return get_edge_graph(generate_static_graph(process))

    @staticmethod
    def from_output_artifacts(outputs: Sequence[Artifact]) -> EdgeGraph:
        """Returns an EdgeGraph from the leaves (outputs) of a computation
        graph."""
        inputs = find_roots(outputs)
        return get_edge_graph(Graph(inputs, outputs))


class Node:
    """An node of the computation graph.

    In our subset of the Open Provenance Model, it may be a Pfocess or an
    Artifact.

    Attributes:
        parents: A mapping from Role to Node.
        children: A mapping from Role to Node.
    """

    def __init__(self):
        self.parents: Mapping[str, Node] = {}
        self.children: Mapping[str, Node] = {}


class Artifact(Node):
    """An OPM Artifact representing the input or output of an OPM Process.

    An Artifact wraps some internal data. Passing the data between artifacts
    and processes is handled by the optex library and should not be done by
    the user -- it is not supported for users to directly access an Artifact's
    data."""

    def __init__(self, data: Any):
        super().__init__()
        self._data: Any = data
        self._entered_scope: Optional[Process] = None
        self.name = None

    def _add_child(self, child: Process, role: str) -> None:
        """Add an edge connecting this Artifact to a Process which consumes it
        as an input argument. This corresponds to a "used" edge in the OPM.

        If this Artifact is an input argument to a composition process and is
        also directly used by another processed within the composition, it will
        not include the composed process as a child. Only the composition
        process will be a child.

        Args:
            child: The Process which consumes this Artifact.
            role: The name of the function parameter this Artifact is mapped
                to. This also corresponds to an OPM edge role."""
        if not self._entered_scope:
            self.children[role] = child
        else:
            self._entered_scope._add_child_process(child)

    def _add_parent(self, parent: Process, role: str) -> None:
        """Add an edge connecting this Artifact to the Process which generates
        it.

        An Artifact can only have one parent.

        Args:
            parent: The Process which creates the artifact.
            role: The name of the function return parameter. This is specified
                as an argument to the function's optex decorator function."""
        self.parents[role] = parent

    def _enter_scope(self, process: Process) -> None:
        """Used during graph construction to indicate that this Artifact is the
        input of a composition process.

        It records the composition Process that forms the containing scope for
        this data and influences the way edges are constructed with this
        Artifact in the future.

        Args:
            process: The composition Process whose scope this Artifact just
                entered."""
        self._entered_scope = process

    def _leave_scope(self) -> None:
        """Used during graph construction to indicate that this Artifact is the
        output of a composition process.

        It rewires the edges of this artifact so that it removes the composed
        process that produced it as a parent, leaving only the composition
        process.
        """
        inner_role = list(self.parents.keys())[0]
        del self.parents[inner_role]


class Process(Node):
    """An OPM Process representing a function accepting and returning OPM
    Artifact objects.

    OPM Processes must be static functions since their behavior must be
    immutable.

    Attributes:
        child_processes: If this Process is the composition of many other
            Processes, these are the edges to the heads of the subgraph it
            contains."""

    def __init__(self, transformation: Callable[..., Any]):
        super().__init__()
        self._transformation = transformation
        self.child_processes = []
        self.name = transformation.__name__

    def _add_child(self, child: Artifact, role: str) -> None:
        """Add an edge connecting this Process to an Artifact it produces as a
        return value.

        The OPM Edge type this corresponds to is "wasGeneratedBy", and the edge
        is identical to the Artifact's corresponding parent edge.

        Each return Artifact has a parameter name specified by the `returns`
        argument of the `optex_process` and `optex_composition` decorator
        functions. The parameter name is also the OPM role for this Edge.

        Args:
            child: Artifact generated by this Process.
            role: The OPM role of the generated Artifact. This corresponds to
                the paramter name specified by the `returns` argument of the
                optex decorator functions."""
        self.children[role] = child

    def _add_parent(self, parent: Artifact, role: str) -> None:
        """Add an edge connecting this Process to an Artifact it consumes as an
        input argument.

        The OPM Edge type this corresponds to is "used", and the edge is
        identical to the Artifact's corresponding child edge.

        Args:
            child: Artifact used by this Process.
            role: The OPM role of the consumed Artifact. This corresponds to
                the paramter name for the Artifact in the wrapped
                transformation function."""
        self.parents[role] = parent

    def _add_child_process(self, process: Process) -> None:
        """Add an edge connecting this Process to a Process it triggers the
        beginning or completion of.

        The OPM Edge type this corresponds to is "wasTriggeredBy".

        These edges are used to define subgraphs contained within a composition
        Process. The composition Process has "wasTriggeredBy" edges to the
        heads of the graph it contains, and the leaves of the subgraph all have
        "wasTriggeredBy" edges to the original composition Process that
        contains them.

        Args:
            process: The Process contained by this composition Process."""
        self.child_processes.append(process)


class OptexProcess(Protocol):
    """An type class for optex process transformations."""

    def transform(
        *args: Artifact, **kwargs: Artifact
    ) -> Union[Artifact, Tuple[Artifact]]:
        """Transforms some input Artifacts into other Artifacts while
        constructing an optex graph describing the computation."""


class Transformation(Protocol):
    """A type class for transformations before being wrapped as optex
    processes."""

    def transform(*args: Any, **kwargs: Any) -> Union[Any, Tuple[Any]]:
        """Transforms some inputs into some outputs."""


def optex_composition(
    returns: Union[str, Sequence[str]]
) -> OptexProcess.transform:
    """Converts a python function which composes other OptexProcesses
    into an OptexProcess.

    It wraps the original transformation function such that its inputs and
    outputs are Artifacts and calling the OptexProcess automatically constructs
    a computation graph within its scope.

    The python function it converts must directly accept and return Artifacts.
    This allows the function to call other OptexProcess functions, since
    otherwise there would be no way to pass in the needed Artifact inputs to
    the composed functions.

    Python functions which compose other OptexProcesses cannot contain any
    computation on the data contained by Artifacts. When constructing an
    OptexProcess, users must choose between composing other OptexProcesses (via
    optex_composition) or performing computation on the data contained in each
    Artifact (via optex_process).

    Args:
        returns: A string or sequence of strings describing the names (OPM
            roles) of the Artifacts the Process returns.

    Returns:
        A new OptexProcess wrapping the decorated transformation function."""

    def decorator(
        transformation: Transformation.transform,
    ) -> OptexProcess.transform:
        @wraps(transformation)
        def transformation_as_optex_process(
            *artifact_args: Artifact,
            **artifact_kwargs: Artifact,
        ) -> Union[Artifact, Tuple[Artifact]]:
            process = Process(transformation)
            bound_args = inspect.Signature.from_callable(transformation).bind(
                *artifact_args, **artifact_kwargs
            )
            bound_args.apply_defaults()
            bound_args = bound_args.arguments

            for role, arg in bound_args.items():
                if not isinstance(arg, Artifact):
                    raise ValueError(
                        f"Expected argument {role} to have type Artifact, but "
                        f"it had type {type(arg)}"
                    )
                arg._add_child(process, role)
                arg._enter_scope(process)
                process._add_parent(arg, role)

            results = transformation(**bound_args)

            if isinstance(results, Tuple):
                for i, artifact in enumerate(results):
                    if not isinstance(artifact, Artifact):
                        raise ValueError(
                            "Expected the composition to return an Artifact, "
                            "but it returned an object of type "
                            f"{type(returns)}"
                        )
                    artifact._leave_scope()
                    artifact._add_parent(process, returns[i])
                    process._add_child(artifact, returns[i])
                return results
            else:
                if not isinstance(results, Artifact):
                    raise ValueError(
                        "Expected the composition to return an Artifact, but "
                        f"it returned an object of type {type(returns)}"
                    )
                results._leave_scope()
                results._add_parent(process, returns)
                process._add_child(results, returns)
                return results

        return transformation_as_optex_process

    return decorator


def optex_process(returns: Union[str, Sequence[str]]) -> OptexProcess:
    """Converts a python function which performs computation on data contained
    within some Artifacts into an OptexProcess.

    It wraps the original transformation function such that its inputs and
    outputs are Artifacts and calling the OptexProcess automatically constructs
    a computation graph within its scope.

    The python function it converts must not accept Artifacts, but instead the
    data that the Artifacts will be wrapped around. This allows the function to
    directly manipulate the data.

    Python functions which perform computation in raw data cannot make any
    calls to other OptexProcesses. When constructing an OptexProcess, users
    must choose between composing other OptexProcesses (via optex_composition)
    or performing computation on the data contained in each Artifact (via
    optex_process).

    Args:
        returns: A string or sequence of strings describing the names (OPM
            roles) of the Artifacts the Process returns.

    Returns:
        A new OptexProcess wrapping the decorated transformation function."""

    def decorator(transformation: Transformation) -> OptexProcess:
        @wraps(transformation)
        def transformation_as_optex_process(
            *artifact_args: Artifact, **artifact_kwargs: Artifact
        ) -> Union[Artifact, Tuple[Artifact]]:
            process = Process(transformation)
            bound_args = inspect.Signature.from_callable(transformation).bind(
                *artifact_args, **artifact_kwargs
            )
            bound_args.apply_defaults()
            bound_args = bound_args.arguments

            kwargs = {}

            for role, arg in bound_args.items():
                if not isinstance(arg, Artifact):
                    raise ValueError(
                        f"Expected argument {role} to have type Artifact, but "
                        f"it had type {type(arg)}"
                    )
                arg._add_child(process, role)
                process._add_parent(arg, role)
                kwargs[role] = arg._data

            if not _DRY_RUN:
                results = transformation(**kwargs)
            else:
                if isinstance(returns, str):
                    results = None
                else:  # must be a list or sequence of strings
                    results = [None for _ in returns]

            if isinstance(results, Tuple):
                artifact_results = []
                for i, result in enumerate(results):
                    artifact = Artifact(result)
                    artifact._add_parent(process, returns[i])
                    process._add_child(artifact, returns[i])
                    artifact_results.append(artifact)
                return artifact_results
            else:
                artifact = Artifact(results)
                artifact._add_parent(process, returns)
                process._add_child(artifact, returns)
                return artifact

        return transformation_as_optex_process

    return decorator
