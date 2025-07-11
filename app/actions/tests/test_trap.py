import pytest
from datetime import datetime, timedelta, timezone

from app.actions.tests.factories import TrapFactory


@pytest.mark.asyncio
async def test_trap_convert_to_utc():
    """
    Test trap.convert_to_utc
    """

    # Setup mock trap
    mock_trap = TrapFactory.create(
        trap_id="test_trap_id_001",
        sequence=1,
        latitude=10.0,
        longitude=20.0,
        deploy_datetime_utc="2023-01-01T00:00:00Z",
        retrieved_datetime_utc="2023-01-02T00:00:00Z",
        status="retrieved",
    )

    datetime_obj = datetime.now(timezone.utc)
    datetime_with_seconds_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    parsed_datetime = mock_trap.convert_to_utc(datetime_with_seconds_str)
    datetime_without_fractional = datetime_obj.replace(microsecond=0)

    assert parsed_datetime
    assert parsed_datetime == datetime_without_fractional
    assert parsed_datetime.strftime("%Y-%m-%d %H:%M:%S") == datetime_with_seconds_str

    # Test with datetime with fractional seconds
    datetime_with_fractional_seconds_str = datetime_obj.strftime("%Y-%m-%d %H:%M:%S.%f")
    parsed_datetime_with_fractional_seconds = mock_trap.convert_to_utc(
        datetime_with_fractional_seconds_str
    )

    assert parsed_datetime_with_fractional_seconds
    assert parsed_datetime_with_fractional_seconds == datetime_obj
    assert (
        parsed_datetime_with_fractional_seconds.strftime("%Y-%m-%d %H:%M:%S.%f")
        == datetime_with_fractional_seconds_str
    )