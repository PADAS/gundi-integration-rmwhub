from typing import List
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
    er_token: pydantic.SecretStr = pydantic.Field(
        ...,
        title="EarthRanger API Token",
        description="API token used to authenticate with EarthRanger Gear API.",
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
    share_with: List[str] = pydantic.Field(
        title="rmwhub \"Share with\" field",
        description="Value to use for the \"share_with\" field when sharing data with rmwHUB.",
        default=""
    )
    minutes_to_sync: int = pydantic.Field(
        title="Minutes to Sync",
        description = "Number of minutes of data to pull from RMW Hub",
        default = 30
    )