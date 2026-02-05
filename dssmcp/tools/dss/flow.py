from ...server import mcp
from ...utils import _get_impersonated_dss_client


@mcp.tool
def get_flow_graph(project_key: str) -> dict:
    """
    Get the structure of a project's Flow graph in left-to-right traversal order.

    Each node represents a dataset, recipe, managed folder, saved model, or other flow item.
    Nodes are returned in topological order so the graph can be read left-to-right.

    :param str project_key: The project key whose flow to retrieve
    :returns: A dict with project_key and an ordered list of nodes, where each node contains
        ref (name), type (e.g. COMPUTABLE_DATASET, RUNNABLE_RECIPE), predecessors, and successors
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    graph = project.get_flow().get_graph()
    nodes = graph.get_items_in_traversal_order(as_type="dict")

    return {
        "project_key": project_key,
        "nodes": nodes,
    }
