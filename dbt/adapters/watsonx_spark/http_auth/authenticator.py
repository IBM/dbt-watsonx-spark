from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class Authenticator(ABC):
    def __init__(self, profile: Optional[Dict[str, Any]]):
        self.profile = profile or {}
        self.type = self.profile.get("type")
        self._token: Optional[str] = None
        self._valid_till: Optional[str] = None

    @abstractmethod
    def Authenticate(self) -> Any:
        """Hook to allow authenticators to mutate transports."""
        raise NotImplementedError


def get_authenticator(authProfile: Optional[Dict[str, Any]], host: str, uri: Optional[str]):
    # Import lazily to avoid circular import during module initialization
    from dbt.adapters.watsonx_spark.http_auth.wxd_authenticator import WatsonxData

    return WatsonxData(authProfile or {}, host, uri)
