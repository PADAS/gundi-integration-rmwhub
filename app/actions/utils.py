
from typing import Tuple

from gundi_client_v2 import GundiClient
from gundi_core import schemas
from app.services.utils import find_config_for_action
from app.actions.buoy.types import Environment
from gundi_core.schemas.v2 import Integration

LOAD_BATCH_SIZE = 100

def generate_batches(iterable, n=LOAD_BATCH_SIZE):
    for i in range(0, len(iterable), n):
        yield iterable[i : i + n]


async def get_er_token_and_site(
    integration: Integration, environment: Environment
) -> Tuple[str, str]:
    _client = GundiClient()
    connection_details = await _client.get_connection_details(integration.id)

    destination = (
        destination
        for destination in connection_details.destinations
        if environment.value in destination.name
    ).__next__()

    destination_details = await _client.get_integration_details(destination.id)
    auth_config = find_config_for_action(
        configurations=destination_details.configurations,
        action_id="auth",
    )

    auth_config = schemas.v2.ERAuthActionConfig.parse_obj(auth_config.data)
    if auth_config:
        return auth_config.token, destination_details.base_url
    return None, None
