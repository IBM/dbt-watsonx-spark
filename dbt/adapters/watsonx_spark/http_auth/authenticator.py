from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union


class Authenticator(ABC):
    def __init__(self, profile: Optional[Dict[str, Any]]):
        self.profile = profile or {}
        self.type = self.profile.get("type")
        self._token: Optional[str] = None
        self._valid_till: Optional[str] = None

    @abstractmethod
    def Authenticate(self, transport: Any) -> Any:
        """Hook to allow authenticators to mutate transports."""
        raise NotImplementedError


def get_authenticator(
    authProfile: Optional[Union[Dict[str, Any], str]], host: Optional[str], uri: Optional[str]
) -> Authenticator:
    # Import lazily to avoid circular import during module initialization
    from dbt.adapters.watsonx_spark.http_auth.wxd_authenticator import WatsonxData
    profile_dict: Dict[str, Any]
    if isinstance(authProfile, dict):
        profile_dict = authProfile
    elif authProfile is None:
        profile_dict = {}
    else:
        profile_dict = {"type": str(authProfile)}
    return WatsonxData(profile_dict, host or "", uri)
