import dataiku
from ...server import mcp


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
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
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
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        project = client.get_project(project_key)
        return project.get_metadata()


@mcp.tool
def get_project_permissions(project_key: str) -> dict:
    """
    Get the permissions attached to this project

    :param str project_key: the project key of the desired project
    :returns: A dict containing the owner and the permissions, as a list of pairs of group name and permission type
    :rtype: dict
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        project = client.get_project(project_key)
        return project.get_permissions()


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
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
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
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
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
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        project = client.get_project(project_key)
        # Get raw list items (not as_type="objects") - returns list of dicts
        items = project.client._perform_json(
            "GET",
            "/projects/%s/datasets/" % project_key,
            params={"foreign": include_shared, "tags": []},
        )
        return items


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
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        project = client.get_project(project_key)
        # Get raw list items - returns list of dicts
        return project.client._perform_json("GET", "/projects/%s/recipes/" % project_key)


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
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        project = client.get_project(project_key)
        return project.client._perform_json("GET", "/projects/%s/scenarios/" % project_key)


########################################################
# Jobs
########################################################

@mcp.tool
def list_project_jobs(project_key: str) -> list:
    """
    List the jobs in this project

    :param str project_key: the project key of the desired project
    :returns: a list of the jobs, each one as a python dict, containing both the definition and the state
    :rtype: list
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        project = client.get_project(project_key)
        return project.list_jobs()


########################################################
# ML Tasks & Analyses
########################################################

@mcp.tool
def list_project_ml_tasks(project_key: str) -> list:
    """
    List the ML tasks in this project

    :param str project_key: the project key of the desired project
    :returns: the list of the ML tasks summaries, each one as a python dict
    :rtype: list
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        project = client.get_project(project_key)
        return project.list_ml_tasks()


@mcp.tool
def list_project_analyses(project_key: str) -> list:
    """
    List the visual analyses in this project

    :param str project_key: the project key of the desired project
    :returns: the list of the visual analyses summaries, each one as a python dict
    :rtype: list
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        project = client.get_project(project_key)
        return project.list_analyses()


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
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
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
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        project = client.get_project(project_key)
        return project.list_managed_folders()
