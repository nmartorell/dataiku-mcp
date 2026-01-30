from ...server import mcp
from ...utils import _get_impersonated_dss_client

########################################################
# Project Lifecycle
########################################################


@mcp.tool
def move_project_to_folder(project_key: str, destination_folder_id: str) -> dict:
    """
    Move a project to a different project folder.

    :param str project_key: The project key of the project to move
    :param str destination_folder_id: The ID of the destination folder (use 'ROOT' for root folder)
    :returns: A dict confirming the move with project_key and destination folder info
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    destination = client.get_project_folder(destination_folder_id)
    project.move_to_folder(destination)
    return {
        "success": True,
        "project_key": project_key,
        "destination_folder_id": destination_folder_id,
        "destination_folder_path": destination.get_path(),
    }


@mcp.tool
def delete_project(
    project_key: str,
    clear_managed_datasets: bool = False,
    clear_output_managed_folders: bool = False,
    clear_job_and_scenario_logs: bool = True,
) -> dict:
    """
    Delete a project from the DSS instance.

    .. attention::
        This call requires an API key with admin rights.

    :param str project_key: The project key of the project to delete
    :param bool clear_managed_datasets: Should the data of managed datasets be cleared (defaults to False)
    :param bool clear_output_managed_folders: Should the data of managed folders used as outputs of recipes be cleared (defaults to False)
    :param bool clear_job_and_scenario_logs: Should the job and scenario logs be cleared (defaults to True)
    :returns: A dict with the deletion result
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    result = project.delete(
        clear_managed_datasets=clear_managed_datasets,
        clear_output_managed_folders=clear_output_managed_folders,
        clear_job_and_scenario_logs=clear_job_and_scenario_logs,
        wait=True,
    )
    return {
        "success": True,
        "project_key": project_key,
        "messages": result.get_result() if result else None,
    }


@mcp.tool
def duplicate_project(
    project_key: str,
    target_project_key: str,
    target_project_name: str,
    duplication_mode: str = "MINIMAL",
    export_analysis_models: bool = True,
    export_saved_models: bool = True,
    export_insights_data: bool = True,
    target_project_folder_id: str = None,
) -> dict:
    """
    Duplicate a project to create a new project with the same content.

    :param str project_key: The project key of the source project to duplicate
    :param str target_project_key: The key for the new duplicated project (must be unique)
    :param str target_project_name: The display name for the new duplicated project
    :param str duplication_mode: Controls what data is copied. One of:
        - MINIMAL: Only copy project structure, no data
        - SHARING: Copy structure and set up sharing
        - FULL: Copy structure and all data
        - NONE: Copy structure without any special handling
        (defaults to MINIMAL)
    :param bool export_analysis_models: Whether to include analysis models (defaults to True)
    :param bool export_saved_models: Whether to include saved models (defaults to True)
    :param bool export_insights_data: Whether to include insights data (defaults to True)
    :param str target_project_folder_id: Optional folder ID for the new project (defaults to same folder as source)
    :returns: A dict containing the original and duplicated project's keys
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)

    target_folder = None
    if target_project_folder_id:
        target_folder = client.get_project_folder(target_project_folder_id)

    result = project.duplicate(
        target_project_key=target_project_key,
        target_project_name=target_project_name,
        duplication_mode=duplication_mode,
        export_analysis_models=export_analysis_models,
        export_saved_models=export_saved_models,
        export_insights_data=export_insights_data,
        target_project_folder=target_folder,
    )
    return result


########################################################
# Project Information
########################################################


@mcp.tool
def get_project_summary(project_key: str) -> dict:
    """
    Returns a summary of the project. The summary is a read-only view of some of the state of the project.
    You cannot edit the resulting dict and use it to update the project state on DSS, you must use the other more
    specific methods of this :class:`dataikuapi.dss.project.DSSProject` object

    :param str project_key: the project key of the desired project
    :returns: a dict containing a summary of the project. Each dict contains at least a **projectKey** field
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    return project.get_summary()


@mcp.tool
def get_project_metadata(project_key: str) -> dict:
    """
    Get the metadata attached to this project. The metadata contains label, description
    checklists, tags and custom metadata of the project.

    .. note::
        For more information on available metadata, please see https://doc.dataiku.com/dss/api/latest/rest/

    :param str project_key: the project key of the desired project
    :returns: the project metadata.
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    return project.get_metadata()


@mcp.tool
def set_project_metadata(project_key: str, metadata: dict) -> dict:
    """
    Set the metadata on a project. The metadata contains label, description,
    checklists, tags and custom metadata of the project.

    Usage: First call get_project_metadata to retrieve current metadata,
    modify the desired fields, then pass the updated dict to this function.

    Example metadata structure:
    {
        "label": "Project Display Name",
        "description": "Project description text",
        "tags": ["tag1", "tag2"],
        "checklists": {...},
        "custom": {...}
    }

    :param str project_key: the project key of the project to update
    :param dict metadata: the metadata dict (should be based on output from get_project_metadata)
    :returns: A dict confirming the update
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    project.set_metadata(metadata)
    return {
        "success": True,
        "project_key": project_key,
        "message": "Project metadata updated successfully",
    }


@mcp.tool
def get_project_permissions(project_key: str) -> dict:
    """
    Get the permissions attached to this project

    :param str project_key: the project key of the desired project
    :returns: A dict containing the owner and the permissions, as a list of pairs of group name and permission type
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    return project.get_permissions()


@mcp.tool
def set_project_permissions(project_key: str, permissions: dict) -> dict:
    """
    Set the permissions on a project.

    Usage: First call get_project_permissions to retrieve current permissions,
    modify the desired fields, then pass the updated dict to this function.

    Example permissions structure:
    {
        "owner": "admin",
        "permissions": [
            {
                "group": "data_scientists",
                "readProjectContent": True,
                "readDashboards": True
            },
            {
                "group": "analysts",
                "readProjectContent": True,
                "writeProjectContent": False
            }
        ]
    }

    :param str project_key: the project key of the project to update
    :param dict permissions: the permissions dict (should be based on output from get_project_permissions)
    :returns: A dict confirming the update
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    project.set_permissions(permissions)
    return {
        "success": True,
        "project_key": project_key,
        "message": "Project permissions updated successfully",
    }


@mcp.tool
def get_project_interest(project_key: str) -> dict:
    """
    Get the interest of this project. The interest means the number of watchers and the number of stars.

    :param str project_key: the project key of the desired project
    :returns: a dict object containing the interest of the project with two fields:

        * **starCount**: number of stars for this project
        * **watchCount**: number of users watching this project

    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    return project.get_interest()


@mcp.tool
def get_project_timeline(project_key: str, item_count: int = 100) -> dict:
    """
    Get the timeline of this project. The timeline consists of information about the creation of this project
    (by whom, and when), the last modification of this project (by whom and when), a list of contributors,
    and a list of modifications. This list of modifications contains a maximum of **item_count** elements
    (default to 100). If **item_count** is greater than the real number of modification, **item_count** is adjusted.

    :param str project_key: the project key of the desired project
    :param int item_count: maximum number of modifications to retrieve in the items list

    :returns: a timeline where the top-level fields are :

        * **allContributors**: all contributors who have been involved in this project
        * **items**: a history of the modifications of the project
        * **createdBy**: who created this project
        * **createdOn**: when the project was created
        * **lastModifiedBy**: who modified this project for the last time
        * **lastModifiedOn**: when this modification took place

    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    return project.get_timeline(item_count=item_count)


########################################################
# Datasets
########################################################


@mcp.tool
def list_project_datasets(project_key: str, include_shared: bool = False) -> list:
    """
    List the datasets in this project.

    :param str project_key: the project key of the desired project
    :param boolean include_shared: If **True**, also lists the datasets from other projects that are shared in this project (defaults to **False**).
    :returns: The list of the datasets as dicts
    :rtype: list
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    datasets = project.list_datasets(
        as_type="listitems", include_shared=include_shared
    )

    # remove unnecessary fields from project list that bloat LLM context window
    datasets_summary = []
    for ds in datasets:
        datasets_summary.append(
            {
                "type": ds.get("type", ""),
                "managed": ds["managed"],
                "name": ds.get("name", ""),
                "smartName": ds.get("smartName", ""),
                "formatType": ds.get("formatType", ""),
                "projectKey": ds.get("projectKey", ""),
                "tags": ds.get("tags", []),
                "schema": ds.get("schema", {}),
            }
        )
    return datasets_summary


########################################################
# Recipes
########################################################


@mcp.tool
def list_project_recipes(project_key: str) -> list:
    """
    List the recipes in this project

    :param str project_key: the project key of the desired project
    :returns: The list of the recipes as dicts
    :rtype: list
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    recipes = project.list_recipes()

    # remove unnecessary fields from project list that bloat LLM context window
    recipes_summary = []
    for r in recipes:
        recipes_summary.append(
            {
                "type": r.get("type", ""),
                "name": r.get("name", ""),
                "projectKey": r.get("projectKey", ""),
                "inputs": r.get("inputs", {}),
                "outputs": r.get("outputs", {}),
                "tags": r.get("tags", []),
            }
        )
    return recipes_summary


########################################################
# Scenarios
########################################################


@mcp.tool
def list_project_scenarios(project_key: str) -> list:
    """
    List the scenarios in this project.

    :param str project_key: the project key of the desired project
    :returns: The list of the scenarios as dicts
    :rtype: list
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    return project.list_scenarios()


########################################################
# Jobs
########################################################


@mcp.tool
def list_project_jobs(project_key: str, num_jobs: int = 10) -> list:
    """
    List the jobs in this project

    :param str project_key: the project key of the desired project
    :param int num_jobs: number of jobs to return
    :returns: a list of the jobs, each one as a python dict, containing both the definition and the state
    :rtype: list
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    return project.list_jobs()[0:num_jobs]


########################################################
# ML Tasks & Analyses
########################################################


@mcp.tool
def list_project_ml_tasks(project_key: str, num_ml_tasks: int = 10) -> list:
    """
    List the ML tasks in this project

    :param str project_key: the project key of the desired project
    :param int num_ml_tasks: number of ML tasks to return
    :returns: the list of the ML tasks summaries, each one as a python dict
    :rtype: list
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    return project.list_ml_tasks()[0:num_ml_tasks]


@mcp.tool
def list_project_analyses(project_key: str, num_analyses: int = 10) -> list:
    """
    List the visual analyses in this project

    :param str project_key: the project key of the desired project
    :param int num_analyses: number of ML analyses to return
    :returns: the list of the visual analyses summaries, each one as a python dict
    :rtype: list
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    return project.list_analyses()[0:num_analyses]


########################################################
# Saved Models
########################################################


@mcp.tool
def list_project_saved_models(project_key: str) -> list:
    """
    List the saved models in this project

    :param str project_key: the project key of the desired project
    :returns: the list of the saved models, each one as a python dict
    :rtype: list
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    return project.list_saved_models()


########################################################
# Managed Folders
########################################################


@mcp.tool
def list_project_managed_folders(project_key: str) -> list:
    """
    List the managed folders in this project

    :param str project_key: the project key of the desired project
    :returns: the list of the managed folders, each one as a python dict
    :rtype: list
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    managed_folders = project.list_managed_folders()

    # remove unnecessary fields from project list that bloat LLM context window
    managed_folders_summary = []
    for mf in managed_folders:
        managed_folders_summary.append(
            {
                "id": mf.get("id", ""),
                "type": mf.get("type", ""),
                "name": mf.get("name", ""),
                "projectKey": mf.get("projectKey", ""),
                "tags": mf.get("tags", []),
                "params": mf.get("params", {}),
            }
        )
    return managed_folders_summary
