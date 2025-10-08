import json
import re
from dbt.adapters.watsonx_spark.http_auth.authenticator import Authenticator
from dbt.adapters.watsonx_spark.http_auth.exceptions import (
    TokenRetrievalError,
    InvalidCredentialsError,
    CatalogDetailsError
)
from dbt.adapters.watsonx_spark.http_auth.status_codes import StatusCodeHandler
from thrift.transport import THttpClient
from venv import logger
import requests
from dbt.adapters.watsonx_spark import __version__
from platform import python_version
import platform
import sys
from typing import Optional, Dict, Any, Tuple


CPD = "CPD"
SAAS = "SASS"
DEFAULT_SASS_URI_VERSION = "v2"
CPD_AUTH_ENDPOINT = "/icp4d-api/v1/authorize"
CPD_AUTH_HEADER = "LhInstanceId"
SASS_AUTH_HEADER = "AuthInstanceId"
DBT_WATSONX_SPARK_VERSION = __version__.version
OS = platform.system()
PYTHON_VERSION = python_version()
USER_AGENT = f"dbt-watsonx-spark/{DBT_WATSONX_SPARK_VERSION} (IBM watsonx.data; Python {PYTHON_VERSION}; {OS})"


class WatsonxDataEnv():
    def __init__(self, envType, authEndpoint, authInstanceHeaderKey):
        self.envType = envType
        self.authEndpoint = authEndpoint
        self.authInstanceHeaderKey = authInstanceHeaderKey


class Token:
    def __init__(self, token):
        self.token = token


class WatsonxData(Authenticator):
    VERSION_REGEX = re.compile(r"/api/(v[0-9]+(?:\.[0-9]+)*)\b(?:/|$)")

    def __init__(self, profile, host, uri):
        self.profile = profile
        self.type = profile.get("type")
        self.instance = profile.get("instance")
        self.user = profile.get("user")
        self.apikey = profile.get("apikey")
        self.host = host
        self.uri = uri
        if self.uri:
            version_from_uri = self._extract_version_from_uri(self.uri)
        else:
            version_from_uri = None

        self.lakehouse_version = (
            version_from_uri
            or DEFAULT_SASS_URI_VERSION
        )
        self.sass_auth_endpoint = f"/lakehouse/api/{self.lakehouse_version}/auth/authenticate"

    def _extract_version_from_uri(self, uri: str) -> Optional[str]:
        """
        Extracts version url like 'v3' or 'v3.1' from paths containing '/api/<version>/'.
        Returns None if not found.
        """
        m = self.VERSION_REGEX.search(uri)
        return m.group(1) if m else None

    def _get_environment(self):
        if "crn" in self.instance:
            return WatsonxDataEnv(SAAS, self.sass_auth_endpoint, SASS_AUTH_HEADER)
        else:
            return WatsonxDataEnv(CPD, CPD_AUTH_ENDPOINT, CPD_AUTH_HEADER)

    def Authenticate(self, transport: THttpClient.THttpClient):
        transport.setCustomHeaders(self._get_headers())
        return transport

    def get_token(self):
        wxd_env = self._get_environment()
        token_obj = self._get_token(wxd_env)
        
        if not token_obj or not hasattr(token_obj, 'token'):
            error_msg = "Failed to retrieve authentication token"
            logger.error(error_msg)
            raise TokenRetrievalError(message=error_msg)
            
        return str(token_obj.token)

    def _get_cpd_token(self, cpd_env):
        cpd_url = f"{self.host}{cpd_env.authEndpoint}"
        response = self._post_request(
            cpd_url, data={"username": self.user, "api_key": self.apikey})
            
        if not response or "token" not in response:
            error_msg = "Invalid response format when retrieving CPD token"
            logger.error(error_msg)
            raise TokenRetrievalError(message=error_msg)
            
        token = Token(response.get("token"))
        return token

    def _get_sass_token(self, sass_env):
        sass_url = f"{self.host}{sass_env.authEndpoint}"
        response = self._post_request(
            sass_url,
            data={
                "username": "ibmlhapikey_" + self.user if self.user != None else "ibmlhapikey",
                "password": self.apikey,
                "instance_name": "",
                "instance_id": self.instance,
            })
            
        if not response:
            error_msg = "Invalid response when retrieving SaaS token"
            logger.error(error_msg)
            raise TokenRetrievalError(message=error_msg)
        
        text = json.dumps(response)
        token_match = re.search(r'"access(?:_)?token"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
        
        if not token_match:
            error_msg = "Could not find access token in response"
            logger.error(error_msg)
            raise TokenRetrievalError(message=error_msg)
            
        token = Token(token_match.group(1))
        return token

    def _post_request(self, url: str, data: dict) -> Dict[str, Any]:
        try:
            header = {"User-Agent": USER_AGENT}
            response = requests.post(url, json=data, headers=header, verify=False)
            
            # Define custom error handlers for specific status codes
            def handle_401(response, error_msg):
                raise InvalidCredentialsError(error_msg)
                
            def handle_other_errors(response, error_msg):
                raise TokenRetrievalError(status_code=response.status_code, message=error_msg)
                
            error_handlers = {
                401: handle_401,
            }
            
            # Use the StatusCodeHandler to handle the response
            success, error_msg = StatusCodeHandler.handle_response(
                response,
                context="Token retrieval",
                error_handlers={
                    401: lambda r, msg: (False, InvalidCredentialsError(msg)),
                    **{code: lambda r, msg: (False, TokenRetrievalError(status_code=r.status_code, message=msg))
                       for code in range(400, 600) if code != 401}
                },
                log_errors=True
            )
            
            if not success:
                if isinstance(error_msg, Exception):
                    raise error_msg
                raise TokenRetrievalError(message=error_msg)
                
            return response.json()
        except requests.exceptions.RequestException as err:
            error_msg = f"Connection error when retrieving token: {err}"
            logger.error(error_msg)
            raise TokenRetrievalError(message=str(err))
        except (TokenRetrievalError, InvalidCredentialsError):
            # Re-raise these exceptions
            raise
        except Exception as err:
            error_msg = f"Unexpected error when retrieving token: {err}"
            logger.error(error_msg)
            raise TokenRetrievalError(message=str(err))

    def _get_headers(self):
        wxd_env = self._get_environment()
        token_obj = self._get_token(wxd_env)
        
        if not token_obj or not hasattr(token_obj, 'token'):
            error_msg = "Failed to retrieve token for request headers"
            logger.error(error_msg)
            raise TokenRetrievalError(message=error_msg)
            
        auth_header = {"Authorization": f"Bearer {token_obj.token}"}
        instance_header = {
            str(wxd_env.authInstanceHeaderKey): str(self.instance)}
        user_agent = {"User-Agent": USER_AGENT}
        headers = {**auth_header, **instance_header, **user_agent}
        return headers

    def _get_token(self, wxd_env):
        try:
            if wxd_env.envType == CPD:
                return self._get_cpd_token(wxd_env)
            elif wxd_env.envType == SAAS:
                return self._get_sass_token(wxd_env)
            else:
                error_msg = f"Unknown environment type: {wxd_env.envType}"
                logger.error(error_msg)
                raise TokenRetrievalError(message=error_msg)
        except Exception as e:
            # If the exception is already one of our custom exceptions, re-raise it
            if isinstance(e, (TokenRetrievalError, InvalidCredentialsError)):
                raise
            # Otherwise, wrap it in a TokenRetrievalError
            error_msg = f"Error retrieving token: {str(e)}"
            logger.error(error_msg)
            raise TokenRetrievalError(message=error_msg) from e

    def get_catlog_details(self, catalog_name) -> Tuple[str, str]:
        wxd_env = self._get_environment()
        url = f"{self.host}/lakehouse/api/{self.lakehouse_version}/catalogs/{catalog_name}"
        
        try:
            result = self._get_token(wxd_env)
            if not result or not hasattr(result, 'token'):
                error_msg = "Failed to retrieve token for catalog details request"
                logger.error(error_msg)
                raise TokenRetrievalError(message=error_msg)
                
            header = {
                'Authorization': f"Bearer {result.token}",
                'accept': 'application/json',
                wxd_env.authInstanceHeaderKey: self.instance,
                "User-Agent": USER_AGENT
            }
            
            response = requests.get(url=url, headers=header, verify=False)
            
            # Define custom error handlers for specific status codes
            error_handlers = {
                401: lambda r, msg: (False, InvalidCredentialsError(msg)),
                404: lambda r, msg: (False, CatalogDetailsError(
                    catalog_name=catalog_name,
                    status_code=r.status_code,
                    message=f"Catalog '{catalog_name}' not found. Please check the catalog name."
                )),
                # For all other error codes
                **{code: lambda r, msg: (False, CatalogDetailsError(
                    catalog_name=catalog_name,
                    status_code=r.status_code,
                    message=msg
                )) for code in range(400, 600) if code not in [401, 404]}
            }
            
            # Use the StatusCodeHandler to handle the response
            success, error_msg = StatusCodeHandler.handle_response(
                response,
                context=f"Catalog details retrieval for '{catalog_name}'",
                error_handlers=error_handlers,
                log_errors=True
            )
            
            if not success:
                if isinstance(error_msg, Exception):
                    raise error_msg
                raise CatalogDetailsError(catalog_name=catalog_name, message=error_msg)
                
            response_json = response.json()
            
            try:
                if self.lakehouse_version == "v2":
                    bucket = response_json.get("associated_buckets")[0]
                    file_format = response_json.get("catalog_type")
                else:
                    bucket = response_json.get("associated_storage")[0]
                    file_format = response_json.get("type")
                    
                if not bucket or not file_format:
                    error_msg = f"Invalid catalog response format. Missing required fields."
                    logger.error(error_msg)
                    raise CatalogDetailsError(catalog_name=catalog_name, message=error_msg)
                    
                return bucket, file_format
            except (IndexError, KeyError) as err:
                error_msg = f"Invalid catalog response format: {err}"
                logger.error(error_msg)
                raise CatalogDetailsError(catalog_name=catalog_name, message=error_msg)
                
        except requests.exceptions.RequestException as err:
            error_msg = f"Connection error when retrieving catalog details: {err}"
            logger.error(error_msg)
            raise CatalogDetailsError(catalog_name=catalog_name, message=str(err))
        except (TokenRetrievalError, InvalidCredentialsError, CatalogDetailsError):
            # Re-raise these exceptions
            raise
        except Exception as err:
            error_msg = f"Unexpected error when retrieving catalog details: {err}"
            logger.error(error_msg)
            raise CatalogDetailsError(catalog_name=catalog_name, message=str(err))
