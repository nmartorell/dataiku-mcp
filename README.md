# Dataiku MCP Server

To run the MCP server in Dataiku:
1. Pull the repo code into a Project Library
2. Create an Flask-backend webapp, and add the following code:

```python
from dssmcp.server import mcp
from fastapi import FastAPI

# Create ASGI app from MCP server
mcp_app = mcp.http_app(path='/mcp')

# FastAPI server
app = FastAPI(title="MCP webapp", lifespan=mcp_app.lifespan)

# NOTE: needed to add this as for the MCP server to work I need to add the "lifespan", but the 
# FastAPI 'app' object is already initialized by DSS.
@app.get('/__ping')
async def ping():
    return "pong"

app.mount("/", mcp_app)
```

The webapp should require DSS authentication (i.e. not be a public webapp).

You can then interact with the MCP server from Claude Code, etc. by pointing to the webapp backend
URL. Create a DSS API key is required for auth.
