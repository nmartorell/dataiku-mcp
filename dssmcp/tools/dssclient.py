from dataikuapi.utils import DataikuException

from ..server import mcp
from ..utils import _get_impersonated_dss_client

########################################################
# Projects
########################################################


@mcp.tool
def list_projects(
    include_location: bool = False, include_description: bool = False
) -> list:
    """
    List the projects.

    :param bool include_location: whether to include project locations (slower)
    :param bool include_description: whether to include project descirptions (a lot more tokens, don't
                                     include unless required)
    :returns: a list of projects, each as a dict. Each dict contains at least a 'projectKey' field
    :rtype: list of dicts
    """
    client = _get_impersonated_dss_client()
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


def _generate_project_key(project_name: str) -> str:
    """
    Generate a project key from a project name.

    Converts letters to uppercase, keeps numbers, replaces all other characters with underscores.

    :param str project_name: The project name to convert
    :returns: A valid project key
    :rtype: str
    """
    result = []
    for char in project_name:
        if char.isalpha():
            result.append(char.upper())
        elif char.isdigit():
            result.append(char)
        else:
            result.append("_")
    return "".join(result)


@mcp.tool
def create_project(
    project_name: str,
    owner: str = None,
    description: str = None,
    project_key: str = None,
    project_folder_id: str = None,
) -> dict:
    """
    Create a new project in Dataiku DSS.

    Note: this call requires an API key with admin rights or the right to create a project.
    If the "project_folder_id" is not provided, then this call also requires an API key with rights
    to "write in root folder" (the error message will be a generic "action forbidden") in this case.

    :param str project_name: The display name for the project (required)
    :param str owner: The login of the owner of the project. If not provided, defaults to the current user.
    :param str description: A description for the project.
    :param str project_key: The unique identifier for the project. If not provided, it is auto-generated
        from the project name (letters uppercase, numbers unchanged, other chars replaced with underscores).
    :param str project_folder_id: The project folder ID in which the project will be created
        (root project folder if not specified).

    :returns: A dict with the created project's key and name.
    :rtype: dict

    .. note::
        If the creation fails because the project_key already exists, re-call this tool with a modified
        project_key by appending "_X" where X is a number starting with 1 (e.g., "MY_PROJECT_1").
        If that also fails, increment X (e.g., "MY_PROJECT_2") and try again.
    """
    client = _get_impersonated_dss_client()

    # Generate project key from name if not provided
    if project_key is None:
        project_key = _generate_project_key(project_name)

    # Get current user as owner if not provided
    if owner is None:
        auth_info = client.get_auth_info(with_secrets=False)
        owner = auth_info.get("authIdentifier")

    # Create the project
    project = client.create_project(
        project_key=project_key,
        name=project_name,
        owner=owner,
        description=description,
        project_folder_id=project_folder_id,
    )

    return {
        "projectKey": project.project_key,
        "name": project_name,
        "owner": owner,
        "message": f"Project '{project_name}' created successfully with key '{project.project_key}'",
    }


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
    client = _get_impersonated_dss_client()
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
    client = _get_impersonated_dss_client()
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
    client = _get_impersonated_dss_client()
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
    client = _get_impersonated_dss_client()
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
    client = _get_impersonated_dss_client()
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
    client = _get_impersonated_dss_client()
    return client.list_plugins()


########################################################
# Users & Groups
########################################################


def _is_user_an_admin() -> bool:
    """
    Determines if the calling user is a Dataiku admin.

    Call this tool to before calling any admin-only tools, assuming you don't know
    if the calling user is an admin.

    :return: A boolean which determines if the calling user is an admin.
    :rtype: bool
    """

    client = _get_impersonated_dss_client()
    try:
        client.list_connections()  # throws exception of not admin
        is_admin = True
    except DataikuException:
        is_admin = False

    return is_admin


@mcp.tool
def list_users(include_settings: bool = False) -> list:
    """
    List all users setup on the DSS instance

    Note: this call requires an API key with admin rights

    :param bool include_settings: Include detailed user settings in the response. Defaults to False.

    :return: A list of users, as a list of dicts
    :rtype: list of dicts
    """
    client = _get_impersonated_dss_client()
    return client.list_users(as_objects=False, include_settings=include_settings)


@mcp.tool
def list_groups() -> list:
    """
    List all groups setup on the DSS instance

    Note: this call requires an API key with admin rights

    :returns: A list of groups, as an list of dicts
    :rtype: list of dicts
    """
    client = _get_impersonated_dss_client()
    return client.list_groups()


@mcp.tool
def get_auth_info() -> dict:
    """
    Returns various information about the user currently authenticated using
    this instance of the API client. Includes if the user is a Dataiku admin.

    This method returns a dict that may contain the following keys (may also contain others):

    * authIdentifier: login for a user, id for an API key
    * groups: list of group names (if  context is an user)

    :returns: a dict
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    user_auth_info = client.get_auth_info(with_secrets=False)
    user_auth_info["isAdmin"] = _is_user_an_admin()

    return user_auth_info


########################################################
# Connections
########################################################


@mcp.tool
def list_connections_names(connection_type: str) -> list:
    """
    List all connections names on the DSS instance.

    :param str connection_type: Returns only connections with this type. Use 'all' if you don't want to filter.

    :return: the list of connections names
    :rtype: List[str]
    """
    client = _get_impersonated_dss_client()
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
    client = _get_impersonated_dss_client()
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
    client = _get_impersonated_dss_client()
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
    client = _get_impersonated_dss_client()
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
    client = _get_impersonated_dss_client()
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
    client = _get_impersonated_dss_client()
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
    client = _get_impersonated_dss_client()
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
    client = _get_impersonated_dss_client()
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
    client = _get_impersonated_dss_client()
    return client.get_licensing_status()


@mcp.tool
def get_sanity_check_codes() -> list:
    """
    Return the list of codes that can be generated by the sanity check.
    This call requires an API key with admin rights.

    :rtype: list[str]
    """
    client = _get_impersonated_dss_client()
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
    client = _get_impersonated_dss_client()
    return client.get_data_quality_status()


########################################################
# General Settings
########################################################

# Allowed settings keys that can be retrieved or modified
ALLOWED_GENERAL_SETTINGS_KEYS = [
    "sparkSettings",
    "containerSettings",
    "defaultK8sClusterId",
    "security",
    "cgroupSettings",
    "maxRunningActivitiesPerJob",
    "maxRunningActivities",
    "maxRunningActivitiesPerKey",
]


@mcp.tool
def get_general_settings(settings_keys: list = None) -> dict:
    """
    Get the general settings of the DSS instance.

    Note: this call requires an API key with admin rights.

    :param list settings_keys: A list of settings keys to retrieve. If not provided, all allowed settings
        are returned. Allowed keys are: sparkSettings, containerSettings, defaultK8sClusterId, security,
        cgroupSettings, maxRunningActivitiesPerJob, maxRunningActivities, maxRunningActivitiesPerKey.

    :returns: A dict containing the requested settings.
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    general_settings = client.get_general_settings()
    all_settings = general_settings.get_raw()

    # If no keys specified, return all allowed settings
    if settings_keys is None:
        settings_keys = ALLOWED_GENERAL_SETTINGS_KEYS

    # Validate requested keys
    invalid_keys = [
        key for key in settings_keys if key not in ALLOWED_GENERAL_SETTINGS_KEYS
    ]
    if invalid_keys:
        return {
            "error": f"Invalid settings keys: {invalid_keys}. Allowed keys are: {ALLOWED_GENERAL_SETTINGS_KEYS}"
        }

    # Return only the requested settings
    return {key: all_settings.get(key) for key in settings_keys if key in all_settings}


@mcp.tool
def set_general_settings(settings: dict) -> dict:
    """
    Set the general settings of the DSS instance.

    Note: this call requires an API key with admin rights.

    Usage: First call get_general_settings to retrieve current settings for the keys you want to modify,
    make your changes to the returned dict, then pass the updated dict to this function.

    :param dict settings: A dict containing the settings to update. Only the following keys are allowed:
        sparkSettings, containerSettings, defaultK8sClusterId, security, cgroupSettings,
        maxRunningActivitiesPerJob, maxRunningActivities, maxRunningActivitiesPerKey.
        Only the keys present in this dict will be updated; other settings remain unchanged.

    :returns: A dict confirming the update with the keys that were modified.
    :rtype: dict
    """
    client = _get_impersonated_dss_client()

    # Validate that only allowed keys are being set
    invalid_keys = [
        key for key in settings.keys() if key not in ALLOWED_GENERAL_SETTINGS_KEYS
    ]
    if invalid_keys:
        return {
            "error": f"Invalid settings keys: {invalid_keys}. Allowed keys are: {ALLOWED_GENERAL_SETTINGS_KEYS}"
        }

    # Get current settings
    general_settings = client.get_general_settings()

    # Update only the provided keys
    for key, value in settings.items():
        general_settings.settings[key] = value

    # Save the settings
    general_settings.save()

    return {
        "message": "General settings updated successfully",
        "updatedKeys": list(settings.keys()),
    }
