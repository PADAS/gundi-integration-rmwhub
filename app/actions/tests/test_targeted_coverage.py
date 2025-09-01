"""
Targeted tests for specific missing coverage lines
"""
import pytest
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime
import pytz
import json
import httpx
from app.actions.rmwhub.adapter import RmwHubAdapter
from app.actions.rmwhub.types import GearSet, Trap


@pytest.fixture
def rmw_adapter(a_good_integration, a_good_configuration):
    """Fixture for RmwHubAdapter instance"""
    return RmwHubAdapter(
        integration_id=a_good_integration.id,
        api_key=a_good_configuration.api_key,
        rmw_url=a_good_configuration.rmw_url,
        er_token="test_token",
        er_destination="https://test.earthranger.com/api/v1.0/",
    )


@pytest.mark.asyncio
async def test_process_upload_successful_flow_with_rmw_updates(rmw_adapter, mocker):
    """Test the complete successful upload flow to cover missing success path lines"""
    start_datetime = datetime.now(pytz.UTC)
    
    # Create a proper mock gear with valid rmwhub_set_id
    mock_device = Mock()
    mock_device.device_id = "device_123"
    mock_device.label = "a"
    mock_device.last_deployed = datetime.now(pytz.UTC)
    mock_device.location = Mock()
    mock_device.location.latitude = 42.0
    mock_device.location.longitude = -70.0
    
    mock_gear = Mock()
    mock_gear.name = "Test Gear"
    mock_gear.additional = {"rmwhub_set_id": "set_123"}
    mock_gear.location = {"latitude": 42.0, "longitude": -70.0}
    mock_gear.last_updated = datetime.now(pytz.UTC)
    mock_gear.devices = [mock_device]
    mock_gear.is_active = True
    
    rmw_adapter.get_er_gears = AsyncMock(return_value=[mock_gear])
    
    # Mock successful upload response
    mock_response = Mock()
    mock_response.status_code = 200
    rmw_adapter.rmw_client.upload_data = AsyncMock(return_value=mock_response)
    
    mock_log_activity = AsyncMock(return_value="test_id")
    mocker.patch("app.actions.rmwhub.adapter.log_action_activity", mock_log_activity)
    
    processed_times, success_times, error_times, errors = await rmw_adapter.process_upload(start_datetime)
    
    # Should have successful upload
    assert len(processed_times) > 0
    assert len(success_times) > 0
    assert len(error_times) == 0
    assert len(errors) == 0


def test_validate_response_with_valid_json(rmw_adapter):
    """Test validate_response with various valid JSON strings"""
    # Test with simple valid JSON
    valid_json1 = '{"key": "value"}'
    assert rmw_adapter.validate_response(valid_json1) is True
    
    # Test with complex valid JSON
    valid_json2 = '{"sets": [{"id": 1, "data": {"nested": true}}]}'
    assert rmw_adapter.validate_response(valid_json2) is True
    
    # Test with empty JSON object
    valid_json3 = '{}'
    assert rmw_adapter.validate_response(valid_json3) is True


def test_clean_data_with_various_inputs(rmw_adapter):
    """Test clean_data method with different input types and edge cases"""
    # Test with integer input (should convert to string)
    result = rmw_adapter.clean_data(12345)
    assert result == "12345"
    
    # Test with float input
    result = rmw_adapter.clean_data(123.45)
    assert result == "123.45"
    
    # Test with boolean input
    result = rmw_adapter.clean_data(True)
    assert result == "True"
    
    # Test with string containing all the characters to be cleaned
    dirty_string = "\n\r\t'\"Hello World\"\t\r\n"
    result = rmw_adapter.clean_data(dirty_string)
    assert "\n" not in result
    assert "\r" not in result
    assert "\t" not in result
    assert "'" not in result
    assert '"' not in result
    assert "Hello World" in result


@pytest.mark.asyncio
async def test_create_rmw_update_gear_with_string_datetime(rmw_adapter):
    """Test gear processing with string datetime that's not a datetime object"""
    mock_device = Mock()
    mock_device.device_id = "device_456"
    mock_device.label = "b"
    mock_device.last_deployed = "2023-01-01T12:00:00Z"  # String, not datetime
    mock_device.location = Mock()
    mock_device.location.latitude = 45.0
    mock_device.location.longitude = -75.0
    
    mock_gear = Mock()
    mock_gear.name = "String Datetime Gear"
    mock_gear.additional = {"rmwhub_set_id": "set_456"}
    mock_gear.location = {"latitude": 45.0, "longitude": -75.0}
    mock_gear.last_updated = "2023-01-01T12:00:00Z"  # String, not datetime
    mock_gear.devices = [mock_device]
    mock_gear.is_active = False  # Test inactive gear path
    
    result = await rmw_adapter._create_rmw_update_from_er_gear(mock_gear, {})
    
    assert result is not None
    assert result.traps[0].status == "retrieved"  # Because is_active is False
    assert result.traps[0].sequence == 2  # Because label is "b"
    assert result.traps[0].is_on_end is False  # Because label is not "a"


@pytest.mark.asyncio  
async def test_create_rmw_update_device_location_fallback(rmw_adapter):
    """Test device location fallback when device doesn't have location"""
    # Create mock device without location attribute
    mock_device = Mock(spec=['device_id', 'label', 'last_deployed'])
    mock_device.device_id = "device_789"
    mock_device.label = "c"
    mock_device.last_deployed = datetime.now(pytz.UTC)
    
    mock_gear = Mock()
    mock_gear.name = "No Device Location Gear"
    mock_gear.additional = {"rmwhub_set_id": "set_789"}
    mock_gear.location = {"latitude": 50.0, "longitude": -80.0}  # Fallback location
    mock_gear.last_updated = datetime.now(pytz.UTC)
    mock_gear.devices = [mock_device]
    mock_gear.is_active = True
    
    result = await rmw_adapter._create_rmw_update_from_er_gear(mock_gear, {})
    
    assert result is not None
    # Should use gear location as fallback
    assert result.traps[0].latitude == 50.0
    assert result.traps[0].longitude == -80.0
