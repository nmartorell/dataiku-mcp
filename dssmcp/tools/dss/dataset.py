from ...server import mcp
from ...utils import _get_impersonated_dss_client

########################################################
# Dataset Creation
########################################################


@mcp.tool
def create_managed_dataset(
    project_key: str,
    dataset_name: str,
    connection: str,
    format_option_id: str = None,
    type_option_id: str = None,
    overwrite: bool = False,
) -> dict:
    """
    Create a new managed dataset in a project.

    A managed dataset is one whose data lifecycle is handled by DSS (as opposed to external
    datasets that point to pre-existing data).

    If you don't know the connection name, call ``get_dataset_settings`` on an existing dataset
    in the project and look at the ``params.connection`` field to find a valid connection name.

    :param str project_key: The project key in which to create the dataset
    :param str dataset_name: The name for the new dataset
    :param str connection: The connection name to store the dataset on (e.g. "filesystem_managed")
    :param str format_option_id: Optional format preset (e.g. "PARQUET", "CSV_EXCEL_GZIP").
        If not provided, the connection's default format is used.
    :param str type_option_id: Optional sub-type of the dataset.
        If not provided, the connection's default type is used.
    :param bool overwrite: If True, overwrite if a dataset with this name already exists (defaults to False)
    :returns: A dict confirming the creation with dataset name and project key
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    builder = project.new_managed_dataset(dataset_name)
    builder.with_store_into(connection, type_option_id=type_option_id, format_option_id=format_option_id)
    builder.create(overwrite=overwrite)
    return {
        "success": True,
        "project_key": project_key,
        "dataset_name": dataset_name,
        "connection": connection,
        "message": f"Managed dataset '{dataset_name}' created successfully on connection '{connection}'",
    }


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


########################################################
# Dataset Data
########################################################

MAX_SAMPLE_ROWS = 1000
DEFAULT_SAMPLE_ROWS = 50


@mcp.tool
def get_dataset_sample(
    project_key: str,
    dataset_name: str,
    num_rows: int = DEFAULT_SAMPLE_ROWS,
    partitions: str = None,
) -> dict:
    """
    Get sample data from a dataset.

    Returns rows from the dataset as a list of dicts, where each dict maps column names to values.

    :param str project_key: The project key containing the dataset
    :param str dataset_name: The name of the dataset
    :param int num_rows: Number of rows to retrieve (default 50, max 1000)
    :param str partitions: Optional partition identifier or comma-separated list of partitions to include
    :returns: A dict containing the schema columns and sample rows
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    dataset = project.get_dataset(dataset_name)

    # Enforce row limits
    if num_rows > MAX_SAMPLE_ROWS:
        num_rows = MAX_SAMPLE_ROWS
    if num_rows < 1:
        num_rows = 1

    # Get schema to map column names
    schema = dataset.get_schema()
    column_names = [col["name"] for col in schema.get("columns", [])]

    # Parse partitions if provided as comma-separated string
    partition_list = None
    if partitions:
        partition_list = [p.strip() for p in partitions.split(",")]

    # Iterate rows and collect sample
    rows = []
    for row_values in dataset.iter_rows(partitions=partition_list):
        # Convert list of values to dict with column names
        row_dict = dict(zip(column_names, row_values))
        rows.append(row_dict)
        if len(rows) >= num_rows:
            break

    return {
        "project_key": project_key,
        "dataset_name": dataset_name,
        "num_rows_requested": num_rows,
        "num_rows_returned": len(rows),
        "columns": column_names,
        "rows": rows,
    }
