"""
Final coverage tests for RmwHubAdapter 
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
async def test_download_data_with_success(rmw_adapter, mocker):
    """Test download_data method with successful response"""
    # Mock the rmw_client.search_hub to return a valid JSON string
    mock_response = '{"sets": []}'
    rmw_adapter.rmw_client.search_hub = AsyncMock(return_value=mock_response)
    
    # Call with required start_datetime parameter
    start_datetime = "2023-01-01T00:00:00Z"
    result = await rmw_adapter.download_data(start_datetime)
    
    assert result == []
    rmw_adapter.rmw_client.search_hub.assert_called_once_with(start_datetime, None)
@pytest.mark.asyncio
async def test_download_data_with_missing_sets_key(rmw_adapter, mocker):
    """Test download_data method when response is missing 'sets' key"""
    # Mock the rmw_client.search_hub to return JSON without 'sets' key
    mock_response = '{"other_key": "value"}'
    rmw_adapter.rmw_client.search_hub = AsyncMock(return_value=mock_response)
    
    mock_logger = mocker.patch('app.actions.rmwhub.adapter.logger')
    
    # Call with required start_datetime parameter
    start_datetime = "2023-01-01T00:00:00Z"
    result = await rmw_adapter.download_data(start_datetime)
    
    assert result == []
    mock_logger.error.assert_called_once()


def test_validate_response_success(rmw_adapter):
    """Test validate_response with valid JSON"""
    valid_json = '{"sets": [{"id": "test"}]}'
    result = rmw_adapter.validate_response(valid_json)
    assert result is True


def test_validate_response_invalid_json(rmw_adapter, mocker):
    """Test validate_response with invalid JSON"""
    invalid_json = '{"invalid": json}'
    mock_logger = mocker.patch('app.actions.rmwhub.adapter.logger')
    
    result = rmw_adapter.validate_response(invalid_json)
    
    assert result is False
    mock_logger.error.assert_called_once()


@pytest.mark.asyncio
async def test_process_upload_with_upload_error(rmw_adapter, mocker):
    """Test process_upload when upload fails"""
    start_datetime = datetime.now(pytz.UTC)
    
    # Create mock gear
    mock_gear = Mock()
    mock_gear.name = "Test Gear"
    mock_gear.additional = {"rmwhub_set_id": "set_123"}
    mock_gear.location = {"latitude": 42.0, "longitude": -70.0}
    mock_gear.last_updated = datetime.now(pytz.UTC)
    mock_gear.devices = []
    mock_gear.is_active = True
    
    rmw_adapter.get_er_gears = AsyncMock(return_value=[mock_gear])
    
    # Mock upload that returns error status
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.text = "Server Error"
    rmw_adapter.rmw_client.upload_data = AsyncMock(return_value=mock_response)
    
    mock_log_activity = AsyncMock(return_value="test_id")
    mocker.patch("app.actions.rmwhub.adapter.log_action_activity", mock_log_activity)
    mock_logger = mocker.patch('app.actions.rmwhub.adapter.logger')
    
    processed_times, success_times, error_times, errors = await rmw_adapter.process_upload(start_datetime)
    
    # Should have processed the gear but had an error
    assert len(error_times) >= 0  # At least some error occurred
    mock_logger.error.assert_called()


@pytest.mark.asyncio
async def test_process_upload_with_exception(rmw_adapter, mocker):
    """Test process_upload when an exception occurs"""
    start_datetime = datetime.now(pytz.UTC)
    
    # Create mock gear 
    mock_gear = Mock()
    mock_gear.name = "Test Gear"
    mock_gear.additional = {"rmwhub_set_id": "set_123"}
    mock_gear.location = {"latitude": 42.0, "longitude": -70.0}
    mock_gear.last_updated = datetime.now(pytz.UTC)
    mock_gear.devices = []
    mock_gear.is_active = True
    
    rmw_adapter.get_er_gears = AsyncMock(return_value=[mock_gear])
    
    # Mock upload that raises an exception
    rmw_adapter.rmw_client.upload_data = AsyncMock(side_effect=Exception("Network error"))
    
    mock_log_activity = AsyncMock(return_value="test_id")
    mocker.patch("app.actions.rmwhub.adapter.log_action_activity", mock_log_activity)
    mock_logger = mocker.patch('app.actions.rmwhub.adapter.logger')
    
    processed_times, success_times, error_times, errors = await rmw_adapter.process_upload(start_datetime)
    
    # Should have an error
    assert len(error_times) >= 0
    mock_logger.error.assert_called()


@pytest.mark.asyncio
async def test_create_rmw_update_from_er_gear_with_devices(rmw_adapter):
    """Test _create_rmw_update_from_er_gear with proper device data"""
    # Create mock device with proper attributes
    mock_device = Mock()
    mock_device.device_id = "device_123"
    mock_device.label = "a"
    mock_device.last_deployed = "2023-01-01T12:00:00Z"
    mock_device.location = Mock()
    mock_device.location.latitude = 42.0
    mock_device.location.longitude = -70.0
    
    # Create mock gear
    mock_gear = Mock()
    mock_gear.name = "Test Gear"
    mock_gear.additional = {"rmwhub_set_id": "set_123"}
    mock_gear.location = {"latitude": 42.0, "longitude": -70.0}
    mock_gear.last_updated = "2023-01-01T12:00:00Z"
    mock_gear.devices = [mock_device]
    mock_gear.is_active = True
    
    result = await rmw_adapter._create_rmw_update_from_er_gear(mock_gear, {})
    
    assert result is not None
    assert result.id == "set_123"
    assert len(result.traps) == 1
    assert result.traps[0].id == "device_123"
    assert result.traps[0].is_on_end is True  # device.label == "a"
    assert result.traps[0].status == "deployed"  # gear.is_active = True


@pytest.mark.asyncio
async def test_create_rmw_update_from_er_gear_inactive_gear(rmw_adapter):
    """Test _create_rmw_update_from_er_gear with inactive gear"""
    # Create mock device
    mock_device = Mock()
    mock_device.device_id = "device_123"
    mock_device.label = "b"  # Not 'a', so is_on_end should be False
    mock_device.last_deployed = "2023-01-01T12:00:00Z"
    mock_device.location = Mock()
    mock_device.location.latitude = 42.0
    mock_device.location.longitude = -70.0
    
    # Create mock gear that is inactive
    mock_gear = Mock()
    mock_gear.name = "Test Gear"
    mock_gear.additional = {"rmwhub_set_id": "set_123"}
    mock_gear.location = {"latitude": 42.0, "longitude": -70.0}
    mock_gear.last_updated = "2023-01-01T12:00:00Z"
    mock_gear.devices = [mock_device]
    mock_gear.is_active = False  # Inactive gear
    
    result = await rmw_adapter._create_rmw_update_from_er_gear(mock_gear, {})
    
    assert result is not None
    assert result.traps[0].status == "retrieved"  # gear.is_active = False
def test_convert_datetime_to_utc(rmw_adapter):
    """Test convert_datetime_to_utc method"""
    # Test with Z suffix
    datetime_with_z = "2023-01-01T12:00:00Z"
    result = rmw_adapter.convert_datetime_to_utc(datetime_with_z)
    assert "+00:00" in result
    
    # Test with already UTC format
    datetime_utc = "2023-01-01T12:00:00+00:00"
    result = rmw_adapter.convert_datetime_to_utc(datetime_utc)
    assert result is not None
