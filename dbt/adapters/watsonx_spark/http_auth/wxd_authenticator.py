from dbt.adapters.watsonx_spark.http_auth.authenticator import Authenticator
from thrift.transport import THttpClient
from venv import logger
import requests


CPD = "CPD"
SAAS = "SASS"
CPD_AUTH_ENDPOINT = "/icp4d-api/v1/authorize"
SASS_AUTH_ENDPOINT = "/lakehouse/api/v2/auth/authenticate"
CPD_AUTH_HEADER = "LhInstanceId"
SASS_AUTH_HEADER = "AuthInstanceId"


class WatsonxDataEnv():
    def __init__(self, envType, authEndpoint, authInstanceHeaderKey):
        self.envType = envType
        self.authEndpoint = authEndpoint
        self.authInstanceHeaderKey = authInstanceHeaderKey


class Token:
    def __init__(self, token):
        self.token = token


class WatsonxData(Authenticator):

    def __init__(self, profile, host):
        self.profile = profile
        self.type = profile.get("type")
        self.instance = profile.get("instance")
        self.user = profile.get("user")
        self.apikey = profile.get("apikey")
        self.host = host

    def _get_environment(self):
        if "crn" in self.instance:
            return WatsonxDataEnv(SAAS, SASS_AUTH_ENDPOINT, SASS_AUTH_HEADER)
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
            response = requests.post(url, json=data, verify=False)
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
        headers = {**auth_header, **instance_header}
        return headers

    def _get_token(self, wxd_env):
        if wxd_env.envType == CPD:
            return self._get_cpd_token(wxd_env)
        elif wxd_env.envType == SAAS:
            return self._get_sass_token(wxd_env)
        
    def get_catlog_details(self, catalog_name):
        wxd_env = self._get_environment()
        url = f"{self.host}/lakehouse/api/v2/catalogs/{catalog_name}"
        result = self._get_token(wxd_env)  
        header = {
          'Authorization': "Bearer {}".format(result.token),
          'accept': 'application/json',
          f"{wxd_env.authInstanceHeaderKey}": f"{self.instance}"
        }   
        try:
            response = requests.get(url=url, headers=header, verify=False)
            if response.status_code != 200:
                logger.error(
                    f"Failed to retrieve get catlog details. Error: Received status code {response.status_code}, {response.content}")
                return
            bucket, file_format = response.json().get("associated_buckets")[0], response.json().get("catalog_type")
            return bucket, file_format
        except Exception as err:
            logger.error(f"Exception caught: {err}")