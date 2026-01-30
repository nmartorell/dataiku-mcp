import dataikuapi
from dataiku.core.intercom import get_backend_url
from fastmcp.server.dependencies import get_http_request


def _get_impersonated_dss_client():
    auth_header = get_http_request().headers["authorization"]
    dss_api_key = auth_header.split()[1]
    dss_backend_url = get_backend_url()
    
    client = dataikuapi.DSSClient(dss_backend_url, dss_api_key)
    client._session.verify = False # in case using self-signed certs for backend
    return client
