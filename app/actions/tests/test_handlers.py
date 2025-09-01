from datetime import datetime, timezone
import pytest
from unittest.mock import Mock, AsyncMock

from app.actions.tests.factories import GearsetFactory, TrapFactory
from app.actions.configurations import AuthenticateConfig
from app.actions.handlers import (
    action_auth, 
    generate_batches, 
    get_er_token_and_site,
    Environment
)


@pytest.mark.asyncio
async def test_handler_action_pull_observations(
    mocker,
    mock_gundi_client_v2,
    mock_publish_event,
    mock_action_handlers,
    mock_gundi_client_v2_class,
    mock_gundi_sensors_client_class,
    mock_get_gundi_api_key,
    a_good_configuration,
    a_good_integration,
    a_good_connection,
    mock_rmw_observations,
):
    fixed_now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    num_gearsets = 5
    num_traps = 2
    items = [
        GearsetFactory.create(
            traps_in_set=num_traps,
            set_id="test_set_id_00" + str(j),
            traps=[
                TrapFactory.create(
                    trap_id="test_trap_id_0" + str(j) + str(i),
                    sequence=i,
                    latitude=10.0,
                    longitude=20.0,
                    deploy_datetime_utc="2023-01-01T00:00:00Z",
                    retrieved_datetime_utc="2023-01-02T00:00:00Z",
                    status="retrieved",
                )
                for i in range(1, num_traps + 1)
            ],
        )
        for j in range(1, num_gearsets + 1)
    ]

    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.actions.handlers.GundiClient", mock_gundi_client_v2_class)
    mocker.patch(
        "app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class
    )
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)

    download_data_mock = mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.download_data",
        return_value=items,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.process_download",
        return_value=mock_rmw_observations,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.process_upload",
        return_value=(0, {}),
    )
    mocker.patch(
        "app.actions.handlers.get_er_token_and_site",
        return_value=("super_secret_token", "er.destination.com"),
    )
    mock_datetime = mocker.patch("app.actions.handlers.datetime")
    mock_datetime.now.return_value = fixed_now

    from app.actions.handlers import action_pull_observations

    action_response = await action_pull_observations(
        a_good_integration, action_config=a_good_configuration
    )

    assert action_response.get("observations_extracted") == (
        len(mock_rmw_observations) * len(a_good_connection.destinations)
    )

    assert download_data_mock.call_count == len(a_good_connection.destinations)


@pytest.mark.asyncio
async def test_action_auth_with_valid_api_key():
    from pydantic import SecretStr
    
    integration = Mock()
    integration.id = "test_integration_id"
    
    action_config = AuthenticateConfig(
        api_key=SecretStr("valid_api_key"),
        rmw_url="https://test.rmwhub.com"
    )
    
    result = await action_auth(integration, action_config)
    
    assert result["valid_credentials"] is True
    assert "some_message" in result


@pytest.mark.asyncio
async def test_action_auth_with_invalid_api_key():
    integration = Mock()
    integration.id = "test_integration_id"
    
    action_config = Mock()
    action_config.api_key = None
    action_config.rmw_url = "https://test.rmwhub.com"
    
    result = await action_auth(integration, action_config)
    
    assert result["valid_credentials"] is False
    assert "some_message" in result


@pytest.mark.asyncio
async def test_action_pull_observations_no_gearsets(
    mocker,
    mock_gundi_client_v2,
    mock_publish_event,
    mock_action_handlers,
    mock_gundi_client_v2_class,
    mock_gundi_sensors_client_class,
    mock_get_gundi_api_key,
    a_good_configuration,
    a_good_integration,
    a_good_connection,
):
    fixed_now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.actions.handlers.GundiClient", mock_gundi_client_v2_class)
    mocker.patch(
        "app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class
    )
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)

    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.download_data",
        return_value=[],
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.process_upload",
        return_value=(0, {}),
    )
    mocker.patch(
        "app.actions.handlers.get_er_token_and_site",
        return_value=("super_secret_token", "er.destination.com"),
    )
    mock_datetime = mocker.patch("app.actions.handlers.datetime")
    mock_datetime.now.return_value = fixed_now

    from app.actions.handlers import action_pull_observations

    result = await action_pull_observations(
        a_good_integration, action_config=a_good_configuration
    )
    
    assert result["observations_extracted"] == 0


@pytest.mark.asyncio
async def test_action_pull_observations_with_rmw_error(
    mocker,
    mock_gundi_client_v2,
    mock_publish_event,
    mock_action_handlers,
    mock_gundi_client_v2_class,
    mock_gundi_sensors_client_class,
    mock_get_gundi_api_key,
    a_good_configuration,
    a_good_integration,
    a_good_connection,
    mock_rmw_observations,
):
    fixed_now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    num_gearsets = 2
    items = [
        GearsetFactory.create(
            traps_in_set=1,
            set_id=f"test_set_id_{j}",
        )
        for j in range(num_gearsets)
    ]

        
    mocker.patch("app.services.action_runner.action_handlers", mock_action_handlers)
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.actions.handlers.GundiClient", mock_gundi_client_v2_class)
    mocker.patch(
        "app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class
    )
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)

    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.download_data",
        return_value=items,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.process_download",
        return_value=mock_rmw_observations,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.process_upload",
        return_value=(5, {"detail": "Some error occurred"}),
    )
    mocker.patch(
        "app.actions.handlers.get_er_token_and_site",
        return_value=("super_secret_token", "er.destination.com"),
    )
    mock_datetime = mocker.patch("app.actions.handlers.datetime")
    mock_datetime.now.return_value = fixed_now

    from app.actions.handlers import action_pull_observations

    result = await action_pull_observations(
        a_good_integration, action_config=a_good_configuration
    )
    
    assert "rmw_updates" in result
    assert result["rmw_updates"]["detail"] == "Some error occurred"


def test_action_pull_observations_24_hour_sync_function_exists():
    from app.actions.handlers import action_pull_observations_24_hour_sync
    import inspect
    
    source = inspect.getsource(action_pull_observations_24_hour_sync.__wrapped__.__wrapped__)
    
    assert "action_pull_observations(integration, action_config)" in source


@pytest.mark.asyncio
async def test_action_pull_observations_24_hour_sync_execution(mocker):
    mock_action_pull = mocker.patch("app.actions.handlers.action_pull_observations", return_value={"result": "success"})
    
    integration = Mock()
    action_config = Mock()
    
    from app.actions.handlers import action_pull_observations_24_hour_sync
    original_func = action_pull_observations_24_hour_sync.__wrapped__.__wrapped__
    
    result = await original_func(integration, action_config)
    
    mock_action_pull.assert_called_once_with(integration, action_config)


def test_generate_batches():
    test_list = list(range(250))
    
    batches = list(generate_batches(test_list, n=100))
    
    assert len(batches) == 3
    assert len(batches[0]) == 100
    assert len(batches[1]) == 100
    assert len(batches[2]) == 50
    
    small_batches = list(generate_batches(test_list, n=30))
    assert len(small_batches) == 9
    

@pytest.mark.asyncio
async def test_get_er_token_and_site(mocker):
    from uuid import uuid4
    
    integration = Mock()
    integration.id = uuid4()
    
    mock_destination = Mock()
    mock_destination.id = uuid4()
    mock_destination.name = "Buoy Dev Environment"
    
    mock_connection_details = Mock()
    mock_connection_details.destinations = [mock_destination]
    
    mock_destination_details = Mock()
    mock_destination_details.base_url = "https://test.earthranger.com/"
    mock_destination_details.configurations = [
        Mock(action_id="auth", data={"token": "test_token"})
    ]
    
    mock_gundi_client = Mock()
    mock_gundi_client.get_connection_details = AsyncMock(return_value=mock_connection_details)
    mock_gundi_client.get_integration_details = AsyncMock(return_value=mock_destination_details)
    
    mocker.patch("app.actions.handlers.GundiClient", return_value=mock_gundi_client)
    mocker.patch("app.actions.handlers.find_config_for_action", return_value=Mock(data={"token": "test_token"}))
    
    token, site = await get_er_token_and_site(integration, Environment.DEV)
    
    assert token == "test_token"
    assert site == "https://test.earthranger.com/"


@pytest.mark.asyncio
async def test_get_er_token_and_site_no_auth_config(mocker):
    from uuid import uuid4
    
    integration = Mock()
    integration.id = uuid4()
    
    mock_destination = Mock()
    mock_destination.id = uuid4()
    mock_destination.name = "Buoy Dev Environment"
    
    mock_connection_details = Mock()
    mock_connection_details.destinations = [mock_destination]
    
    mock_destination_details = Mock()
    mock_destination_details.base_url = "https://test.earthranger.com/"
    mock_destination_details.configurations = []
    
    mock_gundi_client = Mock()
    mock_gundi_client.get_connection_details = AsyncMock(return_value=mock_connection_details)
    mock_gundi_client.get_integration_details = AsyncMock(return_value=mock_destination_details)
    
    mocker.patch("app.actions.handlers.GundiClient", return_value=mock_gundi_client)
    mocker.patch("app.actions.handlers.find_config_for_action", return_value=None)
    
    with pytest.raises(AttributeError):
        await get_er_token_and_site(integration, Environment.DEV)


@pytest.mark.asyncio
async def test_get_er_token_and_site_invalid_auth_config(mocker):
    from uuid import uuid4
    
    integration = Mock()
    integration.id = uuid4()
    
    mock_destination = Mock()
    mock_destination.id = uuid4()
    mock_destination.name = "Buoy Dev Environment"
    
    mock_connection_details = Mock()
    mock_connection_details.destinations = [mock_destination]
    
    mock_destination_details = Mock()
    mock_destination_details.base_url = "https://test.earthranger.com/"
    mock_destination_details.configurations = []
    
    mock_gundi_client = Mock()
    mock_gundi_client.get_connection_details = AsyncMock(return_value=mock_connection_details)
    mock_gundi_client.get_integration_details = AsyncMock(return_value=mock_destination_details)
    
    mocker.patch("app.actions.handlers.GundiClient", return_value=mock_gundi_client)
    mocker.patch("app.actions.handlers.find_config_for_action", return_value=Mock(data={"invalid": "data"}))
    
    mock_auth_config = mocker.patch("app.actions.handlers.schemas.v2.ERAuthActionConfig.parse_obj", return_value=None)
    
    token, site = await get_er_token_and_site(integration, Environment.DEV)
    
    assert token is None
    assert site is None