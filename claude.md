# MCP-DSS: Dataiku Model Context Protocol Server

## Project Overview

This project provides a Model Context Protocol (MCP) server that exposes Dataiku DSS (Data Science Studio) capabilities to Claude and other LLM applications. The MCP server acts as a bridge between Claude and Dataiku instances, enabling AI-assisted data science workflows.

## Architecture

```
mcp-dss/
├── dataikuapi/          # Dataiku Python Client Library (reference, do not edit)
├── dssmcp/              # MCP Server Implementation
│   ├── server.py        # FastMCP server instance
│   └── tools/           # MCP tool implementations
│       ├── dssclient.py # Global-level Dataiku client tools
│       └── dss/         # DSS-specific tools (mirrors dataikuapi/dss structure)
│           ├── __init__.py
│           └── project.py  # Project-level tools
├── requirements.txt     # Python dependencies
├── claude.md            # Project documentation
└── .venv/              # UV virtual environment
```

## Project Status

**Current Implementation:**
- 43 MCP tools implemented covering core Dataiku operations
- **Global-level tools** (25): Projects, Project Folders, Futures, Notebooks, Plugins, Users/Groups, Connections, Code Envs, Clusters, Meanings, Logs, Workspaces, Data Collections, Licensing, and Data Quality
- **Project-level tools** (18): Project lifecycle (move, delete, duplicate), project information (get/set metadata, get/set permissions), datasets, recipes, scenarios, jobs, ML tasks, analyses, saved models, and managed folders
- Tools organized following dataikuapi package structure for consistency
- Uses FastMCP framework (v2.14+)
- Ready for further expansion with dataset-level and recipe-level operations

**Framework:**
- FastMCP server for exposing tools
- Native Dataiku SDK integration
- Production-ready with Gunicorn/Uvicorn support

## Dependencies

```
fastapi~=0.128              # Web framework
fastmcp~=2.14               # MCP framework
uvicorn_worker~=0.4.0       # ASGI worker
gunicorn~=24.1              # Production server
dataiku-api-client==14.3.2  # Official Dataiku client
```

## Dataiku Client Capabilities

The `dataikuapi/` folder contains the comprehensive Dataiku Python client library with 104 Python files. This library is included for reference and should not be edited.

### Core Client Classes

1. **DSSClient** (`dssclient.py`) - Main entry point for Dataiku DSS
2. **GovernClient** (`govern_client.py`) - Data governance operations
3. **FMClient** (`fmclient.py`) - Fleet management for cloud infrastructure
4. **APINodeClient** - API endpoint management

### Available Dataiku Operations

#### Project & Data Management
- Project lifecycle (create, delete, export, import)
- Dataset operations (read, write, schema, sampling)
- Recipe management (SQL, Python, visual recipes)
- Flow management and dependencies
- Managed folders
- Connections to data sources

#### Machine Learning & AI
- ML task creation and training
- Model evaluation and comparison
- Model deployment (API services)
- Saved model management
- LLM integration (llm.py)
- RAG (Retrieval Augmented Generation)
- Document extraction
- AI agents and tools

#### Analytics & Monitoring
- Dashboards
- Insights
- Metrics computation
- Data quality rules
- Unified monitoring
- Statistics worksheets

#### Notebooks & Development
- Jupyter notebooks
- SQL notebooks
- Regular notebooks
- Code Studio
- Web apps

#### Jobs & Execution
- Job creation and execution
- Scenario management
- Continuous activities
- Async operations (Futures API)
- Job monitoring and waiting

#### Governance & Administration
- User and group management
- Role-based permissions
- Governance artifacts
- Governance blueprints
- Artifact search
- Custom pages

#### Cloud Infrastructure (FM Client)
- Cloud account management (AWS, Azure, GCP)
- Instance provisioning
- Virtual network management
- Load balancer management
- Tenant management

#### API & Streaming
- API service deployment
- API endpoints
- Streaming endpoints
- API Node management

#### Plugins & Extensions
- Plugin management
- Project libraries
- Custom integrations
- LangChain integration
- MLflow integration

## Current MCP Implementation

### Server Setup (dssmcp/server.py)

```python
import dataiku
from fastmcp import FastMCP

# Create MCP server
mcp = FastMCP("Tools to interact with a Dataiku instance.")
```

### Implemented Tools

Currently implements 43 tools across 2 modules:

#### Global Tools (dssmcp/tools/dssclient.py) - 25 tools

##### Projects (2 tools)
- **list_project_keys()** - List all project identifiers
- **list_projects(include_location)** - List all projects with details

##### Project Folders (2 tools)
- **list_project_folders()** - List all project folders in a tree structure
- **get_project_folder(folder_id)** - Get details of a specific project folder

##### Futures / Long-running Tasks (2 tools)
- **list_futures(all_users)** - List currently-running long tasks
- **list_running_scenarios(all_users)** - List running scenarios

##### Notebooks (1 tool)
- **list_running_notebooks()** - List currently-running Jupyter notebooks

##### Plugins (1 tool)
- **list_plugins()** - List installed plugins

##### Users & Groups (3 tools)
- **list_users(include_settings)** - List all users (requires admin rights)
- **list_groups()** - List all groups (requires admin rights)
- **get_auth_info(with_secrets)** - Get current authentication info

##### Connections (1 tool)
- **list_connections_names(connection_type)** - List connection names

##### Code Environments (2 tools)
- **list_code_envs()** - List all code environments
- **list_code_env_usages()** - List all code env usages

##### Clusters (1 tool)
- **list_clusters()** - List all clusters

##### Meanings (1 tool)
- **list_meanings()** - List user-defined meanings

##### Logs (1 tool)
- **list_logs()** - List available log files (requires admin rights)

##### Workspaces (1 tool)
- **list_workspaces()** - List workspaces

##### Data Collections (1 tool)
- **list_data_collections()** - List accessible data collections

##### Licensing & Status (2 tools)
- **get_licensing_status()** - Get licensing status
- **get_sanity_check_codes()** - Get sanity check codes (requires admin rights)

##### Data Quality (1 tool)
- **get_data_quality_status()** - Get data quality monitored project statuses

#### Project-Specific Tools (dssmcp/tools/dss/project.py) - 18 tools

These tools operate on specific projects and follow the structure of dataikuapi/dss/project.py:

##### Project Lifecycle (3 tools)
- **move_project_to_folder(project_key, destination_folder_id)** - Move a project to a different folder
- **delete_project(project_key, clear_managed_datasets, clear_output_managed_folders, clear_job_and_scenario_logs)** - Delete a project (requires admin rights)
- **duplicate_project(project_key, target_project_key, target_project_name, duplication_mode, ...)** - Duplicate a project

##### Project Information (7 tools)
- **get_project_summary(project_key)** - Get project summary
- **get_project_metadata(project_key)** - Get project metadata (tags, labels, description)
- **set_project_metadata(project_key, metadata)** - Set project metadata
- **get_project_permissions(project_key)** - Get project permissions
- **set_project_permissions(project_key, permissions)** - Set project permissions
- **get_project_interest(project_key)** - Get project interest (stars, watchers)
- **get_project_timeline(project_key, item_count)** - Get project timeline/history

##### Project Objects (8 tools)
- **list_project_datasets(project_key, include_shared)** - List datasets in project
- **list_project_recipes(project_key)** - List recipes in project
- **list_project_scenarios(project_key)** - List scenarios in project
- **list_project_jobs(project_key)** - List jobs in project
- **list_project_ml_tasks(project_key)** - List ML tasks in project
- **list_project_analyses(project_key)** - List visual analyses in project
- **list_project_saved_models(project_key)** - List saved models in project
- **list_project_managed_folders(project_key)** - List managed folders in project

Example tool implementation:

```python
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
```

## Development Guide

### Adding New MCP Tools

To add new Dataiku operations as MCP tools:

1. Define tool functions in `dssmcp/tools/dssclient.py`
2. Decorate with `@mcp.tool`
3. Use the native Dataiku SDK or dataikuapi client
4. Document parameters and return types clearly

Example pattern:

```python
@mcp.tool
def your_tool_name(param1: str, param2: int) -> dict:
    """
    Tool description for Claude.

    :param param1: Description
    :param param2: Description
    :returns: Description of return value
    """
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        # Implement tool logic
        return result
```

### Authentication

The current implementation uses `dataiku.WebappImpersonationContext()` which requires:
- Running within a Dataiku webapp context OR
- Proper Dataiku API credentials configured

For standalone operation, you may need to use:
```python
import dataikuapi
client = dataikuapi.DSSClient(host, api_key)
```

### Suggested Next Tools to Implement

**✅ Completed:** Project-level operations (13 tools in dssmcp/tools/dss/project.py)

**Next Priority: Dataset Operations (dssmcp/tools/dss/dataset.py)**

Based on dataikuapi/dss/dataset.py, implement tools for:
- `get_dataset_schema(project_key, dataset_name)` - Get dataset schema
- `get_dataset_metadata(project_key, dataset_name)` - Get dataset metadata
- `get_dataset_settings(project_key, dataset_name)` - Get dataset settings
- `get_dataset_last_metric_values(project_key, dataset_name)` - Get dataset metrics

**Future Expansions:**

1. **Recipe Operations (dssmcp/tools/dss/recipe.py)**
   - `get_recipe_settings(project_key, recipe_name)` - Get recipe settings
   - `get_recipe_status(project_key, recipe_name)` - Get recipe status

2. **Scenario Operations (dssmcp/tools/dss/scenario.py)**
   - `get_scenario_settings(project_key, scenario_id)` - Get scenario settings
   - `get_scenario_last_runs(project_key, scenario_id)` - Get recent scenario runs
   - `run_scenario(project_key, scenario_id)` - Trigger scenario execution

3. **Job Operations (dssmcp/tools/dss/job.py)**
   - `get_job_status(project_key, job_id)` - Get job status details
   - `get_job_log(project_key, job_id)` - Get job execution log

4. **Managed Folder Operations (dssmcp/tools/dss/managedfolder.py)**
   - `list_managed_folder_contents(project_key, folder_id)` - List folder contents
   - `get_managed_folder_info(project_key, folder_id)` - Get folder info

**Implementation Pattern:**

All tools follow this pattern:
```python
@mcp.tool
def tool_name(project_key: str, object_name: str) -> dict:
    with dataiku.WebappImpersonationContext() as ctx:
        client = dataiku.api_client()
        project = client.get_project(project_key)
        object = project.get_object(object_name)
        return object.get_info()
```

### Error Handling

Consider adding error handling for common scenarios:
- Invalid project keys
- Authentication failures
- Network timeouts
- Permission errors
- Resource not found

### Testing

When testing tools:
1. Ensure Dataiku instance is accessible
2. Verify API credentials are configured
3. Test with valid project keys and resource names
4. Handle edge cases (empty results, missing resources)

## Reference Documentation

For detailed API documentation, refer to:
- `dataikuapi/dssclient.py` - Main DSSClient class (2351 lines)
- `dataikuapi/dss/` - DSS-specific modules (55 files)
- `dataikuapi/govern_client.py` - Governance operations
- `dataikuapi/fmclient.py` - Fleet management

Each module contains extensive docstrings and type hints.

## Git Configuration

The `.gitignore` excludes:
- `dataikuapi/` - Client library (reference only)
- `.venv/` - Virtual environment
- `.env` - Environment variables

## Running the Server

```bash
# Install dependencies
uv pip install -r requirements.txt

# Run in development mode
python -m dssmcp.server

# Production deployment with Gunicorn
gunicorn -w 4 -k uvicorn_worker.UvicornWorker dssmcp.server:app
```

## Future Enhancements

Potential areas for expansion:
- Complete coverage of DSSClient operations
- Govern client integration for governance workflows
- FM client integration for cloud infrastructure management
- Streaming support for large dataset operations
- Batch operation tools for efficiency
- Webhook support for async job notifications
- Advanced ML workflow orchestration
- Integration with Dataiku's LLM and RAG capabilities
