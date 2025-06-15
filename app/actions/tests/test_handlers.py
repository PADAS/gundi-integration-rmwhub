from datetime import datetime, timedelta, timezone
import pytz
import pytest

from app.actions.tests.factories import GearsetFactory, TrapFactory


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
    """
    Test handler.action_pull_observations
    """
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