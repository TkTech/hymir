"""
Utilities for visualizing the workflow graph.
"""

import enum

import networkx as nx
import matplotlib.pyplot as plt
from networkx.drawing.nx_agraph import graphviz_layout

from hymir.workflow import Workflow


class Layout(enum.Enum):
    """
    Layouts for rendering the workflow graph.
    """

    DOT = "dot"
    CIRCO = "circo"
    FDP = "fdp"
    NEATO = "neato"
    OSAGE = "osage"
    PATCHWORK = "patchwork"
    TWOPI = "twopi"


def render_workflow(
    workflow: Workflow, output: str, *, layout: Layout = Layout.DOT
):
    """
    Render the workflow graph to a file, typically useful for debugging
    complex workflows.

    .. note::

        This function depends on the optional dependencies pygraphviz and
        matplotlib. You can install them with:

        .. code-block:: bash

                pip install pygraphviz matplotlib

    :param workflow: The workflow to render.
    :param output: The path to save the output image to.
    :param layout: The layout engine to use. Defaults to 'dot'.
    """
    plt.clf()

    pos = graphviz_layout(workflow.graph, prog=layout.value)
    nx.draw_networkx_nodes(
        workflow.graph, pos, node_size=700, node_color="skyblue", node_shape=""
    )
    nx.draw_networkx_edges(workflow.graph, pos)
    nx.draw_networkx_labels(
        workflow.graph,
        pos,
        font_size=8,
        labels={
            node: data["job"].name
            for node, data in workflow.graph.nodes(data=True)
        },
        bbox=dict(facecolor="white", alpha=0.7),
    )

    plt.savefig(output)
