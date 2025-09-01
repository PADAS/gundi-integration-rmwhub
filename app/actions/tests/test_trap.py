import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

from app.actions.tests.factories import TrapFactory, GearsetFactory
from app.actions.rmwhub.types import GearSet, Trap


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


@pytest.mark.asyncio
async def test_trap_convert_to_utc_error():
    """
    Test trap.convert_to_utc with invalid datetime string
    """
    mock_trap = TrapFactory.create()
    
    # Test with invalid datetime string - this should raise AttributeError
    # because parse_date returns None and None.replace() fails
    with pytest.raises(AttributeError):
        mock_trap.convert_to_utc("invalid_datetime_string")


@pytest.mark.asyncio
async def test_trap_convert_to_utc_edge_case():
    """
    Test trap.convert_to_utc edge case to cover line 67
    """
    from unittest.mock import patch
    
    mock_trap = TrapFactory.create()
    
    # Mock parse_date to return a datetime that becomes None after replace
    with patch('app.actions.rmwhub.types.parse_date') as mock_parse:
        # Create a mock datetime that will become falsy after replace
        mock_datetime = Mock()
        mock_datetime.replace.return_value = None
        mock_parse.return_value = mock_datetime
        
        # This should now trigger the ValueError on line 67
        with pytest.raises(ValueError, match="Unable to parse datetime string"):
            mock_trap.convert_to_utc("test_string")


@pytest.mark.asyncio
async def test_trap_dict_access():
    """
    Test trap __getitem__ and get methods
    """
    mock_trap = TrapFactory.create(
        trap_id="test_trap_001",
        sequence=1,
        latitude=10.0,
        longitude=20.0,
    )
    
    # Test __getitem__ method (line 32)
    assert mock_trap["id"] == "test_trap_001"
    assert mock_trap["sequence"] == 1
    assert mock_trap["latitude"] == 10.0
    
    # Test get method (line 35)
    assert mock_trap.get("id") == "test_trap_001"
    assert mock_trap.get("longitude") == 20.0


@pytest.mark.asyncio
async def test_trap_hash():
    """
    Test trap __hash__ method
    """
    mock_trap1 = TrapFactory.create(
        trap_id="test_trap_001",
        sequence=1,
        latitude=10.0,
        longitude=20.0,
        deploy_datetime_utc="2023-01-01T00:00:00Z",
    )
    
    mock_trap2 = TrapFactory.create(
        trap_id="test_trap_001",
        sequence=1,
        latitude=10.0,
        longitude=20.0,
        deploy_datetime_utc="2023-01-01T00:00:00Z",
    )
    
    # Test __hash__ method (line 38)
    assert hash(mock_trap1) == hash(mock_trap2)
    
    # Different traps should have different hashes
    mock_trap3 = TrapFactory.create(trap_id="different_id")
    assert hash(mock_trap1) != hash(mock_trap3)


@pytest.mark.asyncio
async def test_trap_get_latest_update_time_deployed():
    """
    Test trap.get_latest_update_time for deployed status
    """
    mock_trap = TrapFactory.create(
        status="deployed",
        deploy_datetime_utc="2023-01-01T12:00:00Z",
    )
    
    # Test deployed status path (line 54)
    update_time = mock_trap.get_latest_update_time()
    assert update_time is not None
    assert isinstance(update_time, datetime)


@pytest.mark.asyncio 
async def test_trap_get_latest_update_time_retrieved():
    """
    Test trap.get_latest_update_time for retrieved status with various datetime combinations
    """
    # Test with retrieved_datetime_utc (line 56)
    mock_trap = TrapFactory.create(
        status="retrieved",
        deploy_datetime_utc="2023-01-01T12:00:00Z",
        surface_datetime_utc="2023-01-02T12:00:00Z",
        retrieved_datetime_utc="2023-01-03T12:00:00Z",
    )
    
    update_time = mock_trap.get_latest_update_time()
    assert update_time is not None
    assert isinstance(update_time, datetime)
    
    # Test with surface_datetime_utc fallback (no retrieved_datetime_utc)
    mock_trap2 = TrapFactory.create(
        status="retrieved",
        deploy_datetime_utc="2023-01-01T12:00:00Z",
        surface_datetime_utc="2023-01-02T12:00:00Z",
        retrieved_datetime_utc=None,
    )
    
    update_time2 = mock_trap2.get_latest_update_time()
    assert update_time2 is not None
    assert isinstance(update_time2, datetime)
    
    # Test with deploy_datetime_utc fallback (no retrieved or surface)
    mock_trap3 = TrapFactory.create(
        status="retrieved",
        deploy_datetime_utc="2023-01-01T12:00:00Z",
        surface_datetime_utc=None,
        retrieved_datetime_utc=None,
    )
    
    update_time3 = mock_trap3.get_latest_update_time()
    assert update_time3 is not None
    assert isinstance(update_time3, datetime)