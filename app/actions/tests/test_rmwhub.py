from datetime import datetime
import json
from unittest.mock import AsyncMock
import pytest
import pytz

from app.actions.rmwhub import RmwHubAdapter, RmwHubClient
from app.actions.tests.factories import GearsetFactory, SubjectFactory, TrapFactory


@pytest.mark.asyncio
async def test_rmwhub_adapter_download_data(
    mocker, a_good_configuration, a_good_integration, get_mock_rmwhub_data
):
    """
    Test rmwhub.download_data
    """

    # Setup mock_rmwhub_client
    mocker.patch(
        "app.actions.rmwhub.RmwHubClient.search_hub",
        return_value=json.dumps(get_mock_rmwhub_data),
    )

    rmwadapter = RmwHubAdapter(
        a_good_integration.id,
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "er.destination.com",
    )
    start_datetime = datetime.now(tz=pytz.utc)
    minute_interval = 5
    rmw_sets = await rmwadapter.download_data(start_datetime, minute_interval)

    assert len(rmw_sets) == 1


@pytest.mark.asyncio
# TODO: rewrite test
async def test_rmw_adapter_process_download(
    mocker, a_good_configuration, a_good_integration
):
    """
    Test rmwhub.process_updates
    """

    # Setup mock_rmwhub_client
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.get_er_subjects",
        return_value=[],
    )

    rmwadapter = RmwHubAdapter(
        a_good_integration.id,
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "er.destination.com",
    )

    num_traps = 2
    num_gearsets = 5
    rmw_sets = [
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
    observations = await rmwadapter.process_download(rmw_sets)

    assert len(observations) == num_gearsets * num_traps


@pytest.mark.asyncio
async def test_rmwhub_adapter_search_hub(mocker, a_good_configuration):
    """
    Test rmwhub.search_hub
    """

    # Setup mock response
    mock_response = {
        "sets": [
            {
                "set_id": "set1",
                "deployment_type": "trawl",
                "traps": [{"sequence": 0, "latitude": 10.0, "longitude": 20.0}],
            },
            {
                "set_id": "set2",
                "deployment_type": "trawl",
                "traps": [{"sequence": 0, "latitude": 30.0, "longitude": 40.0}],
            },
        ]
    }

    mock_response_text = json.dumps(mock_response)

    mocker.patch(
        "app.actions.rmwhub.RmwHubClient.search_hub",
        return_value=mock_response_text,
    )

    rmw_client = RmwHubClient(
        a_good_configuration.api_key, a_good_configuration.rmw_url
    )
    start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    response = await rmw_client.search_hub(start_datetime)

    assert response == mock_response_text


@pytest.mark.asyncio
async def test_rmwhub_adapter_search_hub_failure(mocker, a_good_configuration):
    """
    Test rmwhub.search_hub failure
    """

    mocker.patch(
        "app.actions.rmwhub.RmwHubClient.search_hub",
        return_value="Internal Server Error",
    )

    rmw_client = RmwHubClient(
        a_good_configuration.api_key, a_good_configuration.rmw_url
    )
    start_datetime = datetime.now(tz=pytz.utc)
    minute_interval = 60
    response = await rmw_client.search_hub(start_datetime)

    assert response == "Internal Server Error"


@pytest.mark.asyncio
async def test_rmwhub_adapter_process_upload_insert_success(
    mocker,
    a_good_configuration,
    a_good_integration,
    mock_rmw_upload_response,
):
    """
    Test RmwHubAdapter.process_upload insert operations
    """

    rmw_adapter = RmwHubAdapter(
        a_good_integration.id,
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "er.destination.com",
    )
    start_datetime = datetime.now()
    mock_log_activity = AsyncMock()
    mocker.patch("app.actions.rmwhub.log_action_activity", mock_log_activity)

    # Test handle 0 rmw_sets and 0 ER subjects
    data = []
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.get_er_subjects",
        return_value=data,
    )
    result = {}
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter._upload_data",
        return_value=result,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.search_own",
        return_value=[],
    )

    observations, rmw_response = await rmw_adapter.process_upload(start_datetime)

    assert observations == 0

    # Test handle ER upload success
    num_subjects = 4
    data = [
        SubjectFactory.create(
            name="test_subject_00" + str(i),
        )
        for i in range(1, num_subjects + 1)
    ]
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.get_er_subjects",
        return_value=data,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter._upload_data",
        return_value=mock_rmw_upload_response,
    )
    num_traps = 2
    mock_er_gearset = GearsetFactory.create(
        traps_in_set=num_traps,
        set_id="e_test_set_id_001",
        traps=[
            TrapFactory.create(
                trap_id="e_test_trap_id_00" + str(i),
                sequence=i,
                latitude=10.0,
                longitude=20.0,
                deploy_datetime_utc="2023-01-01T00:00:00Z",
                status="deployed",
            )
            for i in range(1, num_traps + 1)
        ],
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.search_own",
        return_value=[mock_er_gearset],
    )

    observations, rmw_response = await rmw_adapter.process_upload(start_datetime)
    assert observations == num_subjects
    assert rmw_response["trap_count"] == num_subjects

    # Test handle ER upload success with ER Subjects from RMW
    num_subjects = 3
    mock_er_subjects_from_rmw = [
        SubjectFactory.create(
            name="rmwhub_test_subject_00" + str(i),
        )
        for i in range(1, num_subjects + 1)
    ]
    data = mock_er_subjects_from_rmw
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.get_er_subjects",
        return_value=data,
    )
    mock_rmw_upload_response["trap_count"] = 0
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter._upload_data",
        return_value=mock_rmw_upload_response,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.search_own",
        return_value=[mock_er_gearset],
    )

    observations, rmw_response = await rmw_adapter.process_upload(start_datetime)
    assert observations == 0
    assert rmw_response["trap_count"] == 0


@pytest.mark.asyncio
async def test_rmwhub_adapter_process_upload_update_success(
    mocker,
    a_good_configuration,
    a_good_integration,
    mock_rmw_upload_response,
):
    """
    Test RmwHubAdapter.process_upload update operations
    """

    rmw_adapter = RmwHubAdapter(
        a_good_integration.id,
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "http://er.destination.com",
    )
    start_datetime = datetime.now(tz=pytz.utc)
    mock_log_activity = AsyncMock()
    mocker.patch("app.actions.rmwhub.log_action_activity", mock_log_activity)

    # Test handle ER upload success with updates
    num_subjects = 4
    data = [SubjectFactory.create() for _ in range(num_subjects)]
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.get_er_subjects",
        return_value=data,
    )
    mock_rmw_upload_response["trap_count"] = num_subjects
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter._upload_data",
        return_value=mock_rmw_upload_response,
    )

    num_traps = 1
    mock_er_gearset = GearsetFactory.create(
        traps_in_set=num_traps,
        set_id="e_test_set_id_001",
        traps=[
            TrapFactory.create(
                trap_id="e_100###########################",
                sequence=i,
                latitude=10.0,
                longitude=20.0,
                deploy_datetime_utc="2023-01-01T00:00:00Z",
                status="retrieved",
            )
            for i in range(1, num_traps + 1)
        ],
    )
    num_traps = 2
    mock_search_own_response = [
        GearsetFactory.create(
            traps_in_set=num_traps,
            set_id="test_set_id_00" + str(j),
            traps=[
                TrapFactory.create(
                    trap_id="test_trap_id_00" + str(i),
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
        for j in range(2)
    ]
    mock_search_own_response.append(mock_er_gearset)
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.search_own",
        return_value=mock_search_own_response,
    )

    observations, rmw_response = await rmw_adapter.process_upload(start_datetime)
    assert observations == num_subjects
    assert rmw_response


@pytest.mark.asyncio
async def test_rmwhub_adapter_process_upload_failure(
    mocker, a_good_configuration, a_good_integration
):
    """
    Test rmwhub.search_hub no sets
    """

    rmw_adapter = RmwHubAdapter(
        a_good_integration.id,
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "er.destination.com",
    )
    start_datetime = datetime.now(tz=pytz.utc)
    mock_log_activity = AsyncMock()
    mocker.patch("app.actions.rmwhub.log_action_activity", mock_log_activity)

    # Test handle ER upload failure
    data = []
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.get_er_subjects",
        return_value=data,
    )
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter._upload_data",
        return_value={},
    )
    num_traps = 2
    return_gearset = GearsetFactory.create(
        traps_in_set=num_traps,
        set_id="test_set_id_001",
        traps=[
            TrapFactory.create(
                trap_id="test_trap_id_00" + str(i),
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
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.search_own",
        return_value=[return_gearset],
    )

    observations, rmw_response = await rmw_adapter.process_upload(start_datetime)
    assert observations == 0
    assert rmw_response == {}


@pytest.mark.asyncio
async def test_rmwhub_adapter_create_rmw_update_from_simple_er_trawl(
    mocker,
    a_good_integration,
    a_good_configuration,
):
    rmwadapter = RmwHubAdapter(
        a_good_integration.id,
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "er.destination.com",
    )

    # Test create INSERT update (no existing rmwHub gearset)
    subject = SubjectFactory.create()
    gearset_insert = await rmwadapter._create_rmw_update_from_er_subject(subject)

    assert gearset_insert
    assert gearset_insert.traps[0].id
    assert len(gearset_insert.traps[0].id) >= 32

    # Test create UPDATE update (existing rmwHub gearset)
    num_traps = 2
    mock_rmwhub_gearset = GearsetFactory.create(
        traps_in_set=num_traps,
        set_id="test_set_id_001",
        traps=[
            TrapFactory.create(
                trap_id="test_trap_id_00" + str(i),
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
    subject = SubjectFactory.create()
    gearset_update = await rmwadapter._create_rmw_update_from_er_subject(er_subject = subject, rmw_gearset=mock_rmwhub_gearset
    )

    assert gearset_update
    assert gearset_update.traps[0].id
    assert len(gearset_update.traps[0].id) >= 32


@pytest.mark.asyncio
async def test_rmwhub_adapter_create_rmw_update_from_er_subject(
    mocker,
    a_good_integration,
    a_good_configuration,
):
    rmw_adapter = RmwHubAdapter(
        a_good_integration.id,
        a_good_configuration.api_key,
        a_good_configuration.rmw_url,
        "super_secret_token",
        "http://er.destination.com",
    )
    start_datetime = datetime.now(tz=pytz.utc)
    mock_log_activity = AsyncMock()
    mocker.patch("app.actions.rmwhub.log_action_activity", mock_log_activity)

    # Test handle ER upload success with updates
    num_subjects = 4
    data = [
        SubjectFactory.create(
            name="test_subject_00" + str(i),
        )
        for i in range(1, num_subjects + 1)
    ]
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.get_er_subjects",
        return_value=data,
    )

    num_traps = 1
    mock_er_gearset = GearsetFactory.create(
        traps_in_set=num_traps,
        set_id="e_test_set_id_001",
        traps=[
            TrapFactory.create(
                trap_id="e_100###########################",
                sequence=i,
                latitude=10.0,
                longitude=20.0,
                deploy_datetime_utc="2023-01-01T00:00:00Z",
                status="retrieved",
            )
            for i in range(1, num_traps + 1)
        ],
    )
    num_traps = 2
    mock_search_own_response = [
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
        for j in range(2)
    ]
    
    mock_search_own_response.append(mock_er_gearset)
    mocker.patch(
        "app.actions.rmwhub.RmwHubAdapter.search_own",
        return_value=mock_search_own_response,
    )

    uploadMock = AsyncMock()
    mocker.patch("app.actions.rmwhub.RmwHubAdapter._upload_data", uploadMock)
    observations, rmw_response = await rmw_adapter.process_upload(start_datetime)
    assert observations == 4