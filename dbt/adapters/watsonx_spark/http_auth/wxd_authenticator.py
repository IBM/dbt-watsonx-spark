import re
from dbt.adapters.watsonx_spark.http_auth.authenticator import Authenticator
from thrift.transport import THttpClient
from venv import logger
import requests
from dbt.adapters.watsonx_spark import __version__
from platform import python_version
import platform
import sys
from typing import Optional


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
        return str(token_obj.token)

    def _get_cpd_token(self, cpd_env):
        cpd_url = f"{self.host}{cpd_env.authEndpoint}"
        response = self._post_request(
            cpd_url, data={"username": self.user, "api_key": self.apikey})
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
        token = Token(response.get("accessToken"))
        return token

    def _post_request(self, url: str, data: dict):
        try:
            header = {"User-Agent": USER_AGENT}
            response = requests.post(url, json=data, headers= header, verify=False)
            if response.status_code != 200:
                logger.error(
                    f"Failed to retrieve token. Error: Received status code {response.status_code}")
                return
            return response.json()
        except Exception as err:
            logger.error(f"Exception caught: {err}")

    def _get_headers(self):
        wxd_env = self._get_environment()
        token_obj = self._get_token(wxd_env)
        auth_header = {"Authorization": "Bearer {}".format(token_obj.token)}
        instance_header = {
            str(wxd_env.authInstanceHeaderKey): str(self.instance)}
        user_agent = {"User-Agent": USER_AGENT}
        headers = {**auth_header, **instance_header, **user_agent}
        return headers

    def _get_token(self, wxd_env):
        if wxd_env.envType == CPD:
            return self._get_cpd_token(wxd_env)
        elif wxd_env.envType == SAAS:
            return self._get_sass_token(wxd_env)

    def get_catlog_details(self, catalog_name):
        wxd_env = self._get_environment()
        url = f"{self.host}/lakehouse/api/{self.lakehouse_version}/catalogs/{catalog_name}"
        result = self._get_token(wxd_env)
        header = {
            'Authorization': "Bearer {}".format(result.token),
            'accept': 'application/json',
            wxd_env.authInstanceHeaderKey: self.instance,
            "User-Agent": USER_AGENT
        }
        try:
            response = requests.get(url=url, headers=header, verify=False)
            if response.status_code != 200:
                logger.error(
                    f"Failed to retrieve get catlog details. Error: Received status code {response.status_code}, {response.content}")
                return
            bucket, file_format = response.json().get("associated_buckets")[
                0], response.json().get("catalog_type")
            return bucket, file_format
        except Exception as err:
            logger.error(f"Exception caught: {err}")
