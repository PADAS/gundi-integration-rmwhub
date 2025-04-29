import pytest

from app.actions.configurations import PullRmwHubObservationsConfiguration
from gundi_core.schemas.v2 import Connection, ConnectionIntegration
from gundi_core.schemas import IntegrationInformation

from app.actions.tests.factories import SubjectFactory
from ropeless_utils import State


@pytest.fixture
def a_good_state():
    return State(
        er_token="super_secret_token",
        er_site="fishing.pamdas.org",
        event_source="fancy_buoy_company",
        er_event_type="gear_position",
        er_buoy_config=State(
            er_token="an_er_buoy_site_token",
            er_site="https://somewhere.buoyranger.org",
            event_source="some_other_buoy_company",
            er_event_type="gear_position",
        ),
    )


@pytest.fixture
def a_good_integration(a_good_state):
    return IntegrationInformation(
        id="00000000-0000-0000-0000-000000000000",
        state=a_good_state.dict(),
        enabled=True,
        name="Test Integration",
        endpoint="https://someplace.pamdas.orfg/api/v1.0",
        login="test_login",
        password="test_password",
        token="test_token",
    )


@pytest.fixture
def a_good_configuration():
    return PullRmwHubObservationsConfiguration(
        api_key="anApiKey", rmw_url="https://somermwhub.url"
    )


@pytest.fixture
def a_good_connection():
    connection = Connection(
        provider=ConnectionIntegration(
            id="00000000-0000-0000-0000-000000000000",
        ),
        destinations=[
            ConnectionIntegration(
                id="00000000-0000-0000-0000-000000000001",
                name="Buoy Dev",
            ),
            ConnectionIntegration(
                id="00000000-0000-0000-0000-000000000002",
                name="Buoy Staging",
            ),
        ],
    )
    return connection


@pytest.fixture
def get_mock_rmwhub_data():
    return {
        "format_version": 0.1,
        "as_of_utc": "2025-01-03T02:21:57Z",
        "api_key": "apikey",
        "sets": [
            {
                "vessel_id": "test_vessel_id_0",
                "set_id": "test_set_id_0",
                "deployment_type": "trawl",
                "traps_in_set": 2,
                "trawl_path": None,
                "share_with": ["Earth_Ranger"],
                "when_updated_utc": "2025-03-14T16:38:12Z",
                "traps": [
                    {
                        "trap_id": "test_trap_id_0",
                        "sequence": 1,
                        "latitude": 44.63648046,
                        "longitude": -63.58040926,
                        "deploy_datetime_utc": "2024-09-25T13:22:32",
                        "surface_datetime_utc": "2024-09-25T13:22:32",
                        "retrieved_datetime_utc": "2024-09-25T13:23:44",
                        "status": "retrieved",
                        "accuracy": "gps",
                        "release_type": "timed",
                        "is_on_end": True,
                    },
                    {
                        "trap_id": "test_trap_id_1",
                        "sequence": 2,
                        "latitude": 44.63648713,
                        "longitude": -63.58044069,
                        "deploy_datetime_utc": "2024-09-25T13:22:38",
                        "surface_datetime_utc": "2024-09-25T13:22:38",
                        "retrieved_datetime_utc": "2024-09-25T13:23:44",
                        "status": "retrieved",
                        "accuracy": "gps",
                        "release_type": "timed",
                        "is_on_end": True,
                    },
                ],
            },
            {
                "vessel_id": "test_vessel_id_1",
                "set_id": "test_set_id_1",
                "deployment_type": "trawl",
                "traps_in_set": 2,
                "trawl_path": None,
                "share_with": ["Earth_Ranger"],
                "when_updated_utc": "2025-03-14T16:38:12Z",
                "traps": [
                    {
                        "trap_id": "test_trap_id_2",
                        "sequence": 1,
                        "latitude": 44.3748774,
                        "longitude": -68.1630351,
                        "deploy_datetime_utc": "2024-06-10T18:24:46",
                        "surface_datetime_utc": "2024-06-10T18:24:46",
                        "retrieved_datetime_utc": "2024-11-02T12:53:38",
                        "status": "retrieved",
                        "accuracy": "gps",
                        "release_type": "timed",
                        "is_on_end": True,
                    },
                    {
                        "trap_id": "test_trap_id_3",
                        "sequence": 2,
                        "latitude": 44.3754398,
                        "longitude": -68.1630321,
                        "deploy_datetime_utc": "2024-06-10T18:25:08",
                        "surface_datetime_utc": "2024-06-10T18:25:08",
                        "retrieved_datetime_utc": "2024-11-02T12:53:38",
                        "status": "retrieved",
                        "accuracy": "gps",
                        "release_type": "timed",
                        "is_on_end": True,
                    },
                ],
            },
        ],
    }


@pytest.fixture
def mock_rmw_upload_response():
    return {
        "description": "Update confirmation",
        "acknowledged": True,
        "datetime_utc": "2025-01-28T22:04:57Z",
        "trap_count": 4,
        "failed_sets": [],
    }


@pytest.fixture
# TODO: Add an observation for each subject (Trap), currently only 1 per set
def mock_rmw_observations():
    return [
        {
            "name": "test_trap_id_0",
            "source": "rmwhub_test_trap_id_0",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": -5.19816, "lon": 122.8113},
            "additional": {
                "subject_name": "test_trap_id_0",
                "rmwHub_set_id": "test_set_id_0",
                "vessel_id": "test_vessel_id_0",
                "display_id": "test_display_hash_0",
                "event_type": "gear_retrieved",
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": "-5.19816", "longitude": "122.8113"},
                        "device_id": "test_trap_id_0",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": "44.63648713",
                            "longitude": "-63.58044069",
                        },
                        "device_id": "test_trap_id_1",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                ],
            },
        },
        {
            "name": "test_trap_id_2",
            "source": "rmwhub_test_trap_id_2",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": 44.3748774, "lon": -68.1630351},
            "additional": {
                "subject_name": "test_trap_id_2",
                "rmwHub_set_id": "test_set_id_1",
                "vessel_id": "test_vessel_id_1",
                "display_id": "test_display_hash_1",
                "event_type": "gear_retrieved",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": "44.3748774",
                            "longitude": "-68.1630351",
                        },
                        "device_id": "test_trap_id_2",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": "44.3754398",
                            "longitude": "-68.1630321",
                        },
                        "device_id": "test_trap_id_3",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                ],
            },
        },
        {
            "name": "test_trap_id_4",
            "source": "rmwhub_test_trap_id_4",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": 41.4414271, "lon": -70.9058206},
            "additional": {
                "subject_name": "test_trap_id_4",
                "rmwHub_set_id": "test_set_id_2",
                "vessel_id": "test_vessel_id_2",
                "display_id": "test_display_hash_2",
                "event_type": "gear_retrieved",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": "41.4414271",
                            "longitude": "-70.9058206",
                        },
                        "device_id": "test_trap_id_4",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": "41.4383309",
                            "longitude": "-70.9043825",
                        },
                        "device_id": "test_trap_id_5",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                ],
            },
        },
        {
            "name": "test_trap_id_6",
            "source": "rmwhub_test_set_id_2_0",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": 42.0471565, "lon": -70.6253929},
            "additional": {
                "subject_name": "test_trap_id_6",
                "rmwHub_set_id": "test_set_id_3",
                "vessel_id": "test_vessel_id_3",
                "display_id": "test_display_hash_3",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": "42.0471565",
                            "longitude": "-70.6253929",
                        },
                        "device_id": "test_trap_id_6",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                    {
                        "label": "b",
                        "location": {
                            "latitude": "42.0474643",
                            "longitude": "-70.625706",
                        },
                        "device_id": "test_trap_id_7",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    },
                ],
            },
        },
        {
            "name": "test_trap_id_8",
            "source": "rmwhub_test_set_id_3_0",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": "2024-10-22T13:33:15.081704-07:00",
            "location": {"lat": 41.4414271, "lon": -70.9058206},
            "additional": {
                "subject_name": "test_trap_id_8",
                "rmwHub_set_id": "test_set_id_4",
                "vessel_id": "test_vessel_id_4",
                "display_id": "test_display_hash_4",
                "event_type": "gear_deployed",
                "devices": [
                    {
                        "label": "a",
                        "location": {
                            "latitude": "41.4414271",
                            "longitude": "-70.9058206",
                        },
                        "device_id": "test_trap_id_8",
                        "last_updated": "2024-10-22T13:33:15.081704-07:00",
                    }
                ],
            },
        },
    ]


@pytest.fixture
def mock_get_latest_observations_with_duplicates():
    return [
        {
            "id": "081bfce1-e977-46ad-b948-aa90c9283304",
            "location": {"latitude": 20.624751, "longitude": -105.310673},
            "created_at": "2025-01-28T14:51:02.996570-08:00",
            "recorded_at": "2025-01-26T03:20:57+00:00",
            "source": "random-string",
            "exclusion_flags": 0,
            "observation_details": {
                "devices": [
                    {
                        "label": "a",
                        "location": {"latitude": 20.629892, "longitude": -105.318998},
                        "device_id": "F6528E48-39B9-49A8-8F24-0023CF5EE3D7",
                        "last_updated": "2025-01-25T13:22:32+00:00",
                    },
                    {
                        "label": "b",
                        "location": {"latitude": 20.624751, "longitude": -105.310673},
                        "device_id": "BB1ABEBC-13BF-4110-A4A3-DE6C4F7022D4",
                        "last_updated": "2025-01-25T13:22:32+00:00",
                    },
                ],
                "display_id": "84f360b0a8a5",
                "event_type": "gear_deployed",
                "subject_is_active": True,
            },
        }
    ]


@pytest.fixture
def mock_get_latest_observations():
    """
    Fixture that simulates the behavior of get_latest_observations.
    """

    async def _get_latest_observations(self, subject_id: str, page_size: int):
        # You can ignore page_size for this fake implementation
        return SubjectFactory.create_latest_observation(subject_id)

    return _get_latest_observations
