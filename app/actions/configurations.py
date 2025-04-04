# actions/configurations.py
import pydantic
from .core import (
    PullActionConfiguration,
    AuthActionConfiguration,
    ExecutableActionMixin,
)


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    api_key: pydantic.SecretStr = pydantic.Field(
        ...,
        title="rmwHUB API Key",
        description="API key used to read/write data from rmwHUB services.",
        format="password",
    )


class PullRmwHubObservationsConfiguration(PullActionConfiguration):
    api_key: pydantic.SecretStr = pydantic.Field(
        ...,
        title="rmwHUB API Key",
        description="API key used to read/write data from rmwHUB services.",
        format="password",
    )
    rmw_url: str = "https://test.ropeless.network/api/"
