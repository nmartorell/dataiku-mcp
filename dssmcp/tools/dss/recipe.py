from ...server import mcp
from ...utils import _get_impersonated_dss_client

########################################################
# Recipe Type Constraints
########################################################

SINGLE_INPUT_SINGLE_OUTPUT_TYPES = {
    "sync",
    "csync",
    "sort",
    "topn",
    "distinct",
    "prepare",
    "shaker",
    "sampling",
    "grouping",
    "window",
    "pivot",
    "download",
    "export",
    "upsert",
}

SINGLE_INPUT_MULTI_OUTPUT_TYPES = {"split"}

MULTI_INPUT_SINGLE_OUTPUT_TYPES = {
    "join",
    "vstack",
    "generate_features",
    "sql_query",
}

# code recipes are multi input / multi output
CODE_RECIPE_TYPES = {
    "python",
    "r",
    "sql_script",
    "pyspark",
    "sparkr",
    "spark_scala",
    "shell",
    "spark_sql_query",
    "cpython",
    "ksql",
    "streaming_spark_scala",
}

SCORING_TYPES = {
    "prediction_scoring",
    "clustering_scoring",
    "evaluation",
    "standalone_evaluation",
    "nlp_llm_evaluation",
}

OTHER_TYPES = {
    "extract_failed_rows",
    "nlp_llm_rag_embedding",
    "embed_dataset",
    "embed_documents",
}

# Aggregate types (by number of inputs and output)
SINGLE_INPUT_TYPES = (
    SINGLE_INPUT_SINGLE_OUTPUT_TYPES
    | SINGLE_INPUT_MULTI_OUTPUT_TYPES
    | SCORING_TYPES
    | OTHER_TYPES
)

SINGLE_OUTPUT_TYPES = (
    SINGLE_INPUT_SINGLE_OUTPUT_TYPES
    | MULTI_INPUT_SINGLE_OUTPUT_TYPES
    | SCORING_TYPES
    | OTHER_TYPES
)

MULTI_OUTPUT_TYPES = CODE_RECIPE_TYPES | SINGLE_INPUT_MULTI_OUTPUT_TYPES

ALL_RECIPE_TYPES = (
    SINGLE_INPUT_SINGLE_OUTPUT_TYPES
    | SINGLE_INPUT_MULTI_OUTPUT_TYPES
    | MULTI_INPUT_SINGLE_OUTPUT_TYPES
    | CODE_RECIPE_TYPES
    | SCORING_TYPES
    | OTHER_TYPES
)


########################################################
# Recipe Creation
########################################################


@mcp.tool
def create_recipe(
    project_key: str,
    recipe_type: str,
    inputs: list,
    outputs: list,
    recipe_name: str = None,
    code: str = None,
) -> dict:
    """
    Create a new recipe in a project's flow with specified inputs and outputs.
    Inputs and outputs must exist prior to creating the recipe (use create_managed_dataset tool).

    Valid recipe types:

    - **Single-input visual** (exactly 1 input, 1 output): sync, csync, sort, topn, distinct,
      prepare, shaker, sampling, grouping, window, pivot, download, export, upsert
    - **Multi-output visual** (1 input, 1+ output): split
    - **Multi-input visual** (1+ inputs, 1 output): join, vstack, generate_features, sql_query
    - **Code** (any number of inputs/outputs): python, r, sql_script, pyspark, sparkr,
      spark_scala, shell, spark_sql_query, cpython, ksql, streaming_spark_scala
    - **Scoring/evaluation** (1 input, 1 output): prediction_scoring, clustering_scoring, evaluation,
      standalone_evaluation, nlp_llm_evaluation
    - **Other** (1 input, 1 output): extract_failed_rows, nlp_llm_rag_embedding, embed_dataset, embed_documents

    :param str project_key: The project key in which to create the recipe
    :param str recipe_type: The type of recipe to create (see list above)
    :param list inputs: List of input dataset names (strings)
    :param list outputs: List of output specifications. Each element is a dict with:
        - ``name`` (required): the output dataset name
        - ``append`` (optional, default False): whether to append instead of overwrite
    :param str recipe_name: Optional custom recipe name (auto-generated if None)
    :param str code: Optional initial script code (only valid for code recipe types)
    :returns: A dict with the created recipe's name, type, inputs, and outputs
    :rtype: dict
    """
    # Validate recipe type
    if recipe_type not in ALL_RECIPE_TYPES:
        return {
            "error": f"Invalid recipe_type '{recipe_type}'. Must be one of: {sorted(ALL_RECIPE_TYPES)}"
        }

    # Validate input and output counts
    if len(inputs) < 1:
        return {"error": f"Recipes requires at least 1 input, got {len(inputs)}"}
    elif len(outputs) < 1:
        return {"error": f"Recipes requires at least 1 output, got {len(outputs)}"}

    if recipe_type in SINGLE_INPUT_TYPES and len(inputs) > 1:
        return {
            "error": f"Recipe type '{recipe_type}' requires exactly 1 input, got {len(inputs)}"
        }
    if recipe_type in SINGLE_OUTPUT_TYPES and len(outputs) > 1:
        return {
            "error": f"Recipe type '{recipe_type}' requires exactly 1 output, got {len(outputs)}"
        }

    # Validate code param
    if code is not None and recipe_type not in CODE_RECIPE_TYPES:
        return {
            "error": f"The 'code' parameter is only valid for code recipe types, not '{recipe_type}'"
        }

    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)

    # Validate that all referenced datasets already exist in the flow
    existing_datasets = {ds["name"] for ds in project.list_datasets()}
    missing_inputs = [name for name in inputs if name not in existing_datasets]
    if missing_inputs:
        return {
            "error": f"Input dataset(s) not found in project '{project_key}': {missing_inputs}. "
            f"Create them first with create_managed_dataset."
        }
    missing_outputs = [o["name"] for o in outputs if o["name"] not in existing_datasets]
    if missing_outputs:
        return {
            "error": f"Output dataset(s) not found in project '{project_key}': {missing_outputs}. "
            f"Create them first with create_managed_dataset."
        }

    # Build recipe
    builder = project.new_recipe(recipe_type, name=recipe_name)

    for inp in inputs:
        builder.with_input(inp)

    for out in outputs:
        name = out["name"]
        append = out.get("append", False)
        builder.with_output(name, append=append)

    # Add code for code recipes
    if recipe_type in CODE_RECIPE_TYPES and code:
        builder.with_script(code)

    recipe = builder.create()
    return {
        "success": True,
        "project_key": project_key,
        "recipe_name": recipe.recipe_name,
        "recipe_type": recipe_type,
        "inputs": inputs,
        "outputs": [o["name"] for o in outputs],
        "message": f"Recipe '{recipe.recipe_name}' created successfully",
    }


########################################################
# Recipe Settings
########################################################


@mcp.tool
def get_recipe_settings(project_key: str, recipe_name: str) -> dict:
    """
    Get the full settings of a recipe, including its definition and payload.

    For code recipes (python, r, sql_script, etc.), the result includes a ``code`` field
    with the script source. For visual recipes, it includes a ``payload`` field with the
    JSON configuration.

    :param str project_key: The project key containing the recipe
    :param str recipe_name: The name of the recipe
    :returns: A dict with recipe name, type, inputs, outputs, params, and code or payload
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    recipe = project.get_recipe(recipe_name)
    settings = recipe.get_settings()
    recipe_def = settings.get_recipe_raw_definition()
    recipe_type = recipe_def.get("type", "")

    result = {
        "recipe_name": recipe_name,
        "type": recipe_type,
        "inputs": settings.get_recipe_inputs(),
        "outputs": settings.get_recipe_outputs(),
        "params": settings.get_recipe_params(),
    }

    if recipe_type in CODE_RECIPE_TYPES:
        result["code"] = settings.str_payload
    else:
        result["payload"] = settings.get_json_payload()

    return result


@mcp.tool
def set_code_recipe_code(project_key: str, recipe_name: str, code: str) -> dict:
    """
    Set the script code on a code recipe and save it.

    Only works on code recipe types: python, r, sql_script, pyspark, sparkr,
    spark_scala, shell, spark_sql_query, cpython, ksql, streaming_spark_scala.

    :param str project_key: The project key containing the recipe
    :param str recipe_name: The name of the code recipe
    :param str code: The new script code to set on the recipe
    :returns: A dict confirming the update
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    recipe = project.get_recipe(recipe_name)
    settings = recipe.get_settings()
    recipe_type = settings.get_recipe_raw_definition().get("type", "")

    if recipe_type not in CODE_RECIPE_TYPES:
        return {
            "error": f"Recipe '{recipe_name}' is type '{recipe_type}', not a code recipe. "
            f"Use set_visual_recipe_payload for visual recipes."
        }

    settings.str_payload = code
    settings.save()
    return {
        "success": True,
        "project_key": project_key,
        "recipe_name": recipe_name,
        "recipe_type": recipe_type,
        "message": f"Code updated on recipe '{recipe_name}'",
    }


@mcp.tool
def set_visual_recipe_payload(
    project_key: str, recipe_name: str, payload: dict
) -> dict:
    """
    Set the JSON payload on a visual recipe, save it, and apply any required schema updates.

    Only works on non-code recipe types (visual recipes like join, grouping, sync, etc.).
    For code recipes, use ``set_code_recipe_code`` instead.

    Usage: First call ``get_recipe_settings`` to retrieve the current payload, modify the
    desired fields, then pass the updated payload dict to this function.

    :param str project_key: The project key containing the recipe
    :param str recipe_name: The name of the visual recipe
    :param dict payload: The JSON payload to set on the recipe
    :returns: A dict confirming the update, with optional schema update info
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    recipe = project.get_recipe(recipe_name)
    settings = recipe.get_settings()
    recipe_type = settings.get_recipe_raw_definition().get("type", "")

    if recipe_type in CODE_RECIPE_TYPES:
        return {
            "error": f"Recipe '{recipe_name}' is a code recipe (type '{recipe_type}'). "
            f"Use set_code_recipe_code instead."
        }

    settings.set_json_payload(payload)
    settings.save()

    result = {
        "success": True,
        "project_key": project_key,
        "recipe_name": recipe_name,
        "recipe_type": recipe_type,
        "message": f"Payload updated on recipe '{recipe_name}'",
    }

    # Apply schema updates if needed
    try:
        schema_updates = recipe.compute_schema_updates()
        if schema_updates.any_action_required():
            schema_updates.apply()
            result["schema_updates_applied"] = True
    except Exception as e:
        result["schema_update_warning"] = str(e)

    return result


########################################################
# Recipe Execution
########################################################


@mcp.tool
def run_recipe(
    project_key: str,
    recipe_name: str,
    job_type: str = "NON_RECURSIVE_FORCED_BUILD",
    wait: bool = True,
) -> dict:
    """
    Run a recipe by starting a build job on its outputs.

    :param str project_key: The project key containing the recipe
    :param str recipe_name: The name of the recipe to run
    :param str job_type: The job type. One of:
        - NON_RECURSIVE_FORCED_BUILD (default): build only this recipe's outputs
        - RECURSIVE_BUILD: build outputs and any missing upstream dependencies
        - RECURSIVE_FORCED_BUILD: rebuild all upstream dependencies and this recipe's outputs
    :param bool wait: If True (default), wait for the job to complete before returning.
        If False, return immediately with the job ID.
    :returns: A dict with job ID and status information
    :rtype: dict
    """
    client = _get_impersonated_dss_client()
    project = client.get_project(project_key)
    recipe = project.get_recipe(recipe_name)
    job = recipe.run(job_type=job_type, wait=wait, no_fail=True)
    return {
        "job_id": job.id,
        "project_key": project_key,
        "recipe_name": recipe_name,
        "status": job.get_status().get("baseStatus", {}).get("state", "UNKNOWN"),
    }
