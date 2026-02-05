from ...server import mcp
from ...utils import _get_impersonated_dss_client

########################################################
# Dataset Lifecycle
########################################################


@mcp.tool
def delete_dataset(project_key: str, dataset_name: str, drop_data: bool = False) -> dict:
    """
    Delete a dataset from a project.

    :param str project_key: The project key containing the dataset
    :param str dataset_name: The name of the dataset to delete
    :param bool drop_data: Should the data of the dataset be dropped (defaults to False)
    :returns: A dict confirming the deletion
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    dataset = project.get_dataset(dataset_name)
    dataset.delete(drop_data=drop_data)
    return {
        "success": True,
        "project_key": project_key,
        "dataset_name": dataset_name,
        "drop_data": drop_data,
        "message": f"Dataset '{dataset_name}' deleted successfully",
    }


@mcp.tool
def rename_dataset(project_key: str, dataset_name: str, new_name: str) -> dict:
    """
    Rename a dataset with a new specified name.

    :param str project_key: The project key containing the dataset
    :param str dataset_name: The current name of the dataset
    :param str new_name: The new name for the dataset
    :returns: A dict confirming the rename with old and new names
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    dataset = project.get_dataset(dataset_name)
    dataset.rename(new_name)
    return {
        "success": True,
        "project_key": project_key,
        "old_name": dataset_name,
        "new_name": new_name,
        "message": f"Dataset renamed from '{dataset_name}' to '{new_name}'",
    }


########################################################
# Dataset Information
########################################################


@mcp.tool
def get_dataset_settings(project_key: str, dataset_name: str) -> dict:
    """
    Get the settings of a dataset.

    The settings include the dataset type, connection, format, partitioning, and other configuration.

    :param str project_key: The project key containing the dataset
    :param str dataset_name: The name of the dataset
    :returns: A dict containing the dataset settings
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    dataset = project.get_dataset(dataset_name)
    settings = dataset.get_settings()
    return settings.get_raw()


@mcp.tool
def get_dataset_schema(project_key: str, dataset_name: str) -> dict:
    """
    Get the schema of a dataset.

    The schema contains the list of columns with their names and types.

    :param str project_key: The project key containing the dataset
    :param str dataset_name: The name of the dataset
    :returns: A dict containing the schema with the list of columns
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    dataset = project.get_dataset(dataset_name)
    return dataset.get_schema()


@mcp.tool
def get_dataset_metadata(project_key: str, dataset_name: str) -> dict:
    """
    Get the metadata attached to a dataset.

    The metadata contains label, description, checklists, tags and custom metadata of the dataset.

    :param str project_key: The project key containing the dataset
    :param str dataset_name: The name of the dataset
    :returns: A dict containing the dataset metadata
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    dataset = project.get_dataset(dataset_name)
    return dataset.get_metadata()
