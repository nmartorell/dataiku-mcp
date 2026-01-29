import dataiku
from ..server import mcp


@mcp.tool
def list_project_keys() -> list:
    """
    List the project keys (=project identifiers).

    :returns: list of project keys identifiers, as strings
    :rtype: list of strings
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.list_project_keys()
