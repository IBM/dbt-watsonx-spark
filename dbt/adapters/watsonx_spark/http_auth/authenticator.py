from abc import ABC, abstractmethod


class Authenticator(ABC):
    def __init__(self, profile):
        self.profile = profile
        self.type = profile.get("type")
        self._token = None
        self._valid_till = None
        pass

    @abstractmethod
    def Authenticate(self):
        pass

from dbt.adapters.watsonx_spark.http_auth.wxd_authenticator import WatsonxData

def get_authenticator(authProfile, host):
    return WatsonxData(authProfile, host)
