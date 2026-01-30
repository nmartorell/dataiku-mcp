import dataiku

from ..server import mcp

########################################################
# Projects
########################################################


@mcp.tool
def list_project_keys_and_names() -> list:
    """
    List the project keys (=project identifiers) and names.

    :returns: list of dicts mapping project names to project keys
    :rtype: list of dicts
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return [{p["name"]: p["projectKey"]} for p in client.list_projects()]


@mcp.tool
def list_projects(
    include_location: bool = False, include_description: bool = False
) -> list:
    """
    List the projects

    :param bool include_location: whether to include project locations (slower)
    :param bool include_description: whether to include project descirptions (a lot more tokens, don't
                                     include unless required)
    :returns: a list of projects, each as a dict. Each dict contains at least a 'projectKey' field
    :rtype: list of dicts
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        project_list = client.list_projects(include_location=include_location)

        # remove unnecessary fields from project list that bloat LLM context window
        project_list_summary = []
        for p in project_list:
            p_summary = {
                "name": p.get("name", ""),
                "projectKey": p["projectKey"],
                "ownerDisplayName": p.get("ownerDisplayName", ""),
                "ownerLogin": p.get("ownerLogin", ""),
                "tutorialProject": p.get("tutorialProject", ""),
                "tags": p.get("tags", []),
            }
            if include_description:
                p_summary["description"] = p.get("description", [])
            if include_location:
                p_summary["projectLocation"] = p.get("projectLocation", [])

            project_list_summary.append(p_summary)

        return project_list_summary


########################################################
# Project Folders
########################################################


def _get_folder_tree(client, folder, path=""):
    """Recursively get folder tree structure."""
    folder_info = {
        "id": folder.id,
        "name": folder.name,
        "path": path if path else "/",
        "projectKeys": folder.list_project_keys(),
        "children": [],
    }
    for child in folder.list_child_folders():
        child_path = f"{path}/{child.name}" if path else f"/{child.name}"
        folder_info["children"].append(_get_folder_tree(client, child, child_path))
    return folder_info


@mcp.tool
def list_project_folders() -> dict:
    """
    List all project folders in a tree structure starting from the root folder.

    :returns: A dict representing the folder tree with id, name, path, projectKeys, and children
    :rtype: dict
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        root = client.get_root_project_folder()
        return _get_folder_tree(client, root)


@mcp.tool
def get_project_folder(folder_id: str) -> dict:
    """
    Get details of a specific project folder.

    :param str folder_id: The ID of the project folder (use 'ROOT' for root folder)
    :returns: A dict with folder id, name, path, projectKeys, and childrenIds
    :rtype: dict
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        folder = client.get_project_folder(folder_id)
        return {
            "id": folder.id,
            "name": folder.name,
            "path": folder.get_path(),
            "projectKeys": folder.list_project_keys(),
            "childrenIds": folder._data.get("childrenIds", []),
        }


########################################################
# Futures (Long-running tasks)
########################################################


@mcp.tool
def list_futures(all_users: bool = False) -> list:
    """
    List the currently-running long tasks (a.k.a futures)

    :param boolean all_users: if True, returns futures for all users (requires admin privileges). Else, only returns futures for the user associated with the current authentication context (if any)

    :return: list of futures. Each future in the list is a dict. Each dict contains at least a 'jobId' field
    :rtype: list of dict
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.list_futures(as_objects=False, all_users=all_users)


@mcp.tool
def list_running_scenarios(all_users: bool = False) -> list:
    """
    List the running scenarios

    :param boolean all_users: if True, returns scenarios for all users (requires admin privileges). Else, only returns scenarios for the user associated with the current authentication context (if any)

    :return: list of running scenarios, each one as a dict containing at least a "jobId" field for the
        future hosting the scenario run, and a "payload" field with scenario identifiers
    :rtype: list of dicts
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.list_running_scenarios(all_users=all_users)


########################################################
# Notebooks
########################################################


@mcp.tool
def list_running_notebooks() -> list:
    """
    List the currently-running Jupyter notebooks

    :return: list of notebooks. Each item in the list is a dict which contains at least a "name" field.
    :rtype: list of dict
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.list_running_notebooks(as_objects=False)


########################################################
# Plugins
########################################################


@mcp.tool
def list_plugins() -> list:
    """
    List the installed plugins

    :returns: list of dict. Each dict contains at least a 'id' field
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.list_plugins()


########################################################
# Users & Groups
########################################################


@mcp.tool
def list_users(include_settings: bool = False) -> list:
    """
    List all users setup on the DSS instance

    Note: this call requires an API key with admin rights

    :param bool include_settings: Include detailed user settings in the response. Defaults to False.

    :return: A list of users, as a list of dicts
    :rtype: list of dicts
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.list_users(as_objects=False, include_settings=include_settings)


@mcp.tool
def list_groups() -> list:
    """
    List all groups setup on the DSS instance

    Note: this call requires an API key with admin rights

    :returns: A list of groups, as an list of dicts
    :rtype: list of dicts
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.list_groups()


@mcp.tool
def get_auth_info() -> dict:
    """
    Returns various information about the user currently authenticated using
    this instance of the API client.

    This method returns a dict that may contain the following keys (may also contain others):

    * authIdentifier: login for a user, id for an API key
    * groups: list of group names (if  context is an user)

    :returns: a dict
    :rtype: dict
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.get_auth_info(with_secrets=False)


########################################################
# Connections
########################################################


@mcp.tool
def list_connections_names(connection_type: str) -> list:
    """
    List all connections names on the DSS instance.

    :param str connection_type: Returns only connections with this type. Use 'all' if you don't want to filter.
    Note: use 'EC2' for S3 connections.

    :return: the list of connections names
    :rtype: List[str]
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.list_connections_names(connection_type=connection_type)


########################################################
# Code envs
########################################################


@mcp.tool
def list_code_envs() -> list:
    """
    List all code envs setup on the DSS instance

    :returns: a list of code envs. Each code env is a dict containing at least "name", "type" and "language"
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        code_envs = client.list_code_envs(as_objects=False)

        # remove unnecessary fields from project list that bloat LLM context window
        code_envs_summary = []
        for env in code_envs:
            code_envs_summary.append(
                {
                    "envName": env.get("envName", ""),
                    "envLang": env["envLang"],
                    "owner": env.get("owner", ""),
                    "pythonInterpreter": env.get("pythonInterpreter", ""),
                }
            )
        return code_envs_summary


@mcp.tool
def list_code_env_usages() -> list:
    """
    List all usages of a code env in the instance.
    This tool returns a large json, use with caution.

    :return: a list of objects where the code env is used
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.list_code_env_usages()


########################################################
# Clusters
########################################################


@mcp.tool
def list_clusters() -> list:
    """
    List all clusters setup on the DSS instance

    Returns:
        List clusters (name, type, state)
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.list_clusters()


########################################################
# Meanings
########################################################


@mcp.tool
def list_meanings() -> list:
    """
    List all user-defined meanings on the DSS instance

    :returns: A list of meanings. Each meaning is a dict
    :rtype: list of dicts
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.list_meanings()


########################################################
# Logs
########################################################


# TODO: should this tool be removed?
# @mcp.tool
def list_logs() -> list:
    """
    List all available log files on the DSS instance
    This call requires an API key with admin rights

    :returns: A list of log file names
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.list_logs()


########################################################
# Workspaces
########################################################


@mcp.tool
def list_workspaces() -> list:
    """
    List the workspaces

    :returns: The list of workspaces.
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.list_workspaces(as_objects=False)


########################################################
# Data Collections
########################################################


@mcp.tool
def list_data_collections() -> list:
    """
    List the accessible data collections

    :returns: The list of data collections as a list of dicts
    :rtype: a list of dict
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.list_data_collections(as_type="dict")


########################################################
# Licensing & Status
########################################################


@mcp.tool
def get_licensing_status() -> dict:
    """
    Returns a dictionary with information about licensing status of this DSS instance.
    This call requires an API key with admin rights.

    :rtype: dict
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.get_licensing_status()


@mcp.tool
def get_sanity_check_codes() -> list:
    """
    Return the list of codes that can be generated by the sanity check.
    This call requires an API key with admin rights.

    :rtype: list[str]
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.get_sanity_check_codes()


########################################################
# Data Quality
########################################################


@mcp.tool
def get_data_quality_status() -> dict:
    """
    Get the status of data-quality monitored projects, including the count of monitored datasets in Ok/Warning/Error/Empty statuses.

    :returns: The dict of data quality monitored project statuses.
    :rtype: dict with PROJECT_KEY as key
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        return client.get_data_quality_status()
