from datetime import timedelta
import pytest

from app.actions.tests.factories import GearsetFactory, SubjectFactory, TrapFactory


@pytest.mark.asyncio
async def test_trap_create_observations_force_create():
    # Setup mock gear and er_subject
    num_traps = 2
    mock_gear = GearsetFactory.create(
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
                status="deployed",
            )
            for i in range(1, num_traps + 1)
        ],
    )
    mock_gear.deployment_type = "trawl"
    mock_trap = mock_gear.traps[0]

    subject_name = "rmwhub_" + mock_trap.id
    devices = [
        {
            "device_id": subject_name,
            "label": "a",
            "location": {
                "latitude": mock_trap.latitude,
                "longitude": mock_trap.longitude,
            },
            "last_updated": mock_trap.deploy_datetime_utc,
        }
    ]
    mock_er_subject = SubjectFactory.create(
        name=subject_name,
        latitude=mock_trap.latitude,
        longitude=mock_trap.longitude,
        last_updated=mock_trap.deploy_datetime_utc,
        event_type="gear_deployed",
        devices=devices,
    )

    expected_recorded_at = (
        mock_trap.get_latest_update_time() + timedelta(seconds=5)
    ).isoformat()

    observations = await mock_gear.create_observations(mock_er_subject)

    assert len(observations) == 2
    for observation in observations:
        if observation["name"] == "rmwhub_" + mock_trap.id:
            assert observation["recorded_at"] == expected_recorded_at
