"""
Tests específicos para atingir 100% de coverage no adapter.py
Foca nas linhas que ainda não estão cobertas: 257-260, 287-296, 323, 335, 383-384
"""
import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timezone
from uuid import uuid4
import json

from app.actions.rmwhub.adapter import RmwHubAdapter
from app.actions.buoy.types import BuoyGear, ObservationSubject
from gundi_core.schemas.v2.gundi import LogLevel


@pytest.fixture
def rmw_adapter(a_good_integration, a_good_configuration):
    """Create RmwHubAdapter instance for testing"""
    return RmwHubAdapter(
        integration_id=a_good_integration.id,
        api_key=a_good_configuration.api_key,
        rmw_url=a_good_configuration.rmw_url,
        er_token="test_token",
        er_destination="https://test.earthranger.com/api/v1.0/",
    )


@pytest.mark.asyncio
async def test_upload_task_gear_processing_exception(rmw_adapter):
    """Test exception handling during gear processing (lines 257-260)"""
    # Mock gear that will cause exception during processing
    mock_gear = Mock()
    mock_gear.name = "problematic_gear"
    mock_gear.devices = []
    
    # Mock the _create_rmw_update_from_er_gear to raise exception
    with patch.object(rmw_adapter, '_create_rmw_update_from_er_gear', side_effect=Exception("Processing error")):
        with patch.object(rmw_adapter, 'get_er_gears', return_value=[mock_gear]):
            with patch.object(rmw_adapter, 'create_display_id_to_gear_mapping', return_value={}):
                with patch('app.actions.rmwhub.adapter.logger') as mock_logger:
                    with patch('app.actions.rmwhub.adapter.log_action_activity', new_callable=AsyncMock):
                        processed_times, success_times, error_times, errors = await rmw_adapter.process_upload(
                            start_datetime=datetime.now()
                        )
                        
                        # Verify exception was logged and error was recorded
                        mock_logger.error.assert_called_with("Error processing gear problematic_gear: Processing error")
                        assert len(error_times) == 1
                        assert len(errors) == 1
                        assert errors[0][0] == "Error processing gear problematic_gear"
                        assert len(processed_times) == 0
                        assert len(success_times) == 0


@pytest.mark.asyncio
async def test_upload_task_upload_exception(rmw_adapter):
    """Test exception handling during upload to RMW Hub (lines 287-296)"""
    # Create valid gear
    mock_gear = Mock()
    mock_gear.name = "test_gear"
    mock_gear.devices = []
    mock_gear.location = {"latitude": 1.0, "longitude": 2.0}
    mock_gear.last_updated = "2023-01-01T00:00:00Z"
    mock_gear.is_active = True
    
    # Mock successful gear processing but failed upload
    mock_gearset = Mock()
    with patch.object(rmw_adapter, '_create_rmw_update_from_er_gear', return_value=mock_gearset):
        with patch.object(rmw_adapter, 'get_er_gears', return_value=[mock_gear]):
            with patch.object(rmw_adapter, 'create_display_id_to_gear_mapping', return_value={}):
                # Mock upload_data to raise exception
                rmw_adapter.rmw_client.upload_data = AsyncMock(side_effect=Exception("Upload failed"))
                
                with patch('app.actions.rmwhub.adapter.logger') as mock_logger:
                    with patch('app.actions.rmwhub.adapter.log_action_activity', new_callable=AsyncMock):
                        processed_times, success_times, error_times, errors = await rmw_adapter.process_upload(
                            start_datetime=datetime.now()
                        )
                        
                        # Verify upload exception was logged
                        mock_logger.error.assert_called_with("Upload error: Upload failed")
                        assert len(error_times) == 1
                        assert len(errors) == 1
                        assert errors[0][0] == "Upload error"
                        assert len(success_times) == 0


@pytest.mark.asyncio
async def test_upload_task_general_exception(rmw_adapter):
    """Test general exception handling in upload task (lines 287-296)"""
    with patch.object(rmw_adapter, 'get_er_gears', side_effect=Exception("General task error")):
        with patch('app.actions.rmwhub.adapter.logger') as mock_logger:
            with patch('app.actions.rmwhub.adapter.log_action_activity', new_callable=AsyncMock) as mock_log:
                processed_times, success_times, error_times, errors = await rmw_adapter.process_upload(
                    start_datetime=datetime.now()
                )
                
                # Verify general exception was logged
                mock_logger.error.assert_called_with("Error in upload task: General task error")
                mock_log.assert_called_with(
                    integration_id=rmw_adapter.integration_id,
                    action_log_type="upload",
                    level=LogLevel.ERROR,
                    log="Error in upload task: General task error",
                    parent_log_id=mock_log.return_value,
                )
                assert len(error_times) == 1
                assert len(errors) == 1
                assert errors[0][0] == "Upload task error"


@pytest.mark.asyncio
async def test_create_rmw_update_device_without_location(rmw_adapter):
    """Test device processing when device doesn't have location attribute (line 323)"""
    
    # Create a Mock gear with proper attributes
    mock_gear = Mock()
    mock_gear.location = {"latitude": 10.0, "longitude": 20.0}
    mock_gear.last_updated = "2023-01-01T00:00:00Z"
    mock_gear.is_active = True
    mock_gear.name = "test_gear"
    mock_gear.display_id = "test_display_id"
    mock_gear.additional = {
        "rmwhub_set_id": "test_set_id",
        "vessel_id": "test_vessel",
        "deployment_type": "test_deployment"
    }
    
    # Create a device mock 
    mock_device = Mock()
    mock_device.device_id = "device_123"
    mock_device.label = "a"
    mock_device.last_deployed = "2023-01-01T00:00:00Z"
    
    mock_gear.devices = [mock_device]
    
    # Mock hasattr specifically for the location check to return False
    original_hasattr = hasattr
    with patch('builtins.hasattr') as mock_hasattr:
        def hasattr_side_effect(obj, attr):
            if obj is mock_device and attr == 'location':
                return False  # Force the else branch for device.location
            # Use original hasattr for everything else
            return original_hasattr(obj, attr)
        
        mock_hasattr.side_effect = hasattr_side_effect
        
        result = await rmw_adapter._create_rmw_update_from_er_gear(
            mock_gear, {}
        )
        
        # Verify the result exists and trap was created using last_location fallback
        assert result is not None
        assert len(result.traps) == 1
        trap = result.traps[0]
        # The latitude should come from last_location since device has no location
        assert trap.latitude == 10.0
        assert trap.longitude == 20.0


@pytest.mark.asyncio
async def test_create_rmw_update_device_without_device_id(rmw_adapter):
    """Test device processing when device doesn't have device_id attribute (line 335)"""
    
    # Create a Mock gear with proper attributes
    mock_gear = Mock()
    mock_gear.location = {"latitude": 10.0, "longitude": 20.0}
    mock_gear.last_updated = "2023-01-01T00:00:00Z"
    mock_gear.is_active = True
    mock_gear.name = "test_gear"
    mock_gear.display_id = "test_display_id"
    mock_gear.additional = {
        "rmwhub_set_id": "test_set_id",
        "vessel_id": "test_vessel",
        "deployment_type": "test_deployment"
    }
    
    # Create a device mock
    mock_device = Mock()
    mock_device.label = "b"
    mock_device.last_deployed = "2023-01-01T00:00:00Z"
    mock_device.location = Mock()
    mock_device.location.latitude = 5.0
    mock_device.location.longitude = 15.0
    
    mock_gear.devices = [mock_device]
    
    # Mock hasattr specifically for the device_id check to return False
    original_hasattr = hasattr
    with patch('builtins.hasattr') as mock_hasattr:
        def hasattr_side_effect(obj, attr):
            if obj is mock_device and attr == 'device_id':
                return False  # Force the else branch for device.device_id
            # Use original hasattr for everything else
            return original_hasattr(obj, attr)
        
        mock_hasattr.side_effect = hasattr_side_effect
        
        result = await rmw_adapter._create_rmw_update_from_er_gear(
            mock_gear, {}
        )
        
        # Verify the result exists and trap was created using fallback device ID
        assert result is not None
        assert len(result.traps) == 1
        trap = result.traps[0]
        # The device ID should be the fallback pattern "device_0"
        assert trap.id == "device_0"
        assert trap.sequence == 2  # Since label is "b", not "a"


def test_validate_response_empty_string(rmw_adapter):
    """Test validate_response with empty string (lines 383-384)"""
    with patch('app.actions.rmwhub.adapter.logger') as mock_logger:
        result = rmw_adapter.validate_response("")
        
        # Verify empty response is rejected and logged
        assert result is False
        mock_logger.error.assert_called_with("Empty response from RMW Hub API")


def test_validate_response_none(rmw_adapter):
    """Test validate_response with None (lines 383-384)"""
    with patch('app.actions.rmwhub.adapter.logger') as mock_logger:
        result = rmw_adapter.validate_response(None)
        
        # Verify None response is rejected and logged
        assert result is False
        mock_logger.error.assert_called_with("Empty response from RMW Hub API")


@pytest.mark.asyncio
async def test_create_rmw_update_non_datetime_last_updated(rmw_adapter):
    """Test str() conversion for last_updated when it's not datetime or string (line 323)"""
    
    # Create a custom class that doesn't have isoformat and isn't a string
    class CustomTimestamp:
        def __init__(self, value):
            self.value = value
        
        def __str__(self):
            return str(self.value)
    
    # Create a Mock gear with last_updated as custom object (not datetime, not string)
    mock_gear = Mock()
    mock_gear.location = {"latitude": 10.0, "longitude": 20.0}
    mock_gear.last_updated = CustomTimestamp(1672531200)  # Custom object, not datetime or string
    mock_gear.is_active = True
    mock_gear.name = "test_gear"
    mock_gear.display_id = "test_display_id"
    mock_gear.additional = {
        "rmwhub_set_id": "test_set_id",
        "vessel_id": "test_vessel",
        "deployment_type": "test_deployment"
    }
    
    # Create a device mock
    mock_device = Mock()
    mock_device.device_id = "device_123"
    mock_device.label = "a"
    mock_device.last_deployed = "2023-01-01T00:00:00Z"
    mock_device.location = Mock()
    mock_device.location.latitude = 5.0
    mock_device.location.longitude = 15.0
    
    mock_gear.devices = [mock_device]
    
    result = await rmw_adapter._create_rmw_update_from_er_gear(
        mock_gear, {}
    )
    
    # Verify the result exists and last_updated was converted to string
    assert result is not None
    assert result.when_updated_utc == "1672531200"  # Should be string conversion of custom object


@pytest.mark.asyncio
async def test_create_rmw_update_non_datetime_deploy_datetime(rmw_adapter):
    """Test str() conversion for deploy_datetime when it's not datetime or string (line 335)"""
    
    # Create a custom class that doesn't have isoformat and isn't a string
    class CustomTimestamp:
        def __init__(self, value):
            self.value = value
        
        def __str__(self):
            return str(self.value)
    
    # Create a Mock gear with proper attributes
    mock_gear = Mock()
    mock_gear.location = {"latitude": 10.0, "longitude": 20.0}
    mock_gear.last_updated = "2023-01-01T00:00:00Z"
    mock_gear.is_active = True
    mock_gear.name = "test_gear"
    mock_gear.display_id = "test_display_id"
    mock_gear.additional = {
        "rmwhub_set_id": "test_set_id",
        "vessel_id": "test_vessel",
        "deployment_type": "test_deployment"
    }
    
    # Create a device mock with last_deployed as custom object (not datetime, not string)
    mock_device = Mock()
    mock_device.device_id = "device_123"
    mock_device.label = "a"
    mock_device.last_deployed = CustomTimestamp(1672531200)  # Custom object, not datetime or string
    mock_device.location = Mock()
    mock_device.location.latitude = 5.0
    mock_device.location.longitude = 15.0
    
    mock_gear.devices = [mock_device]
    
    result = await rmw_adapter._create_rmw_update_from_er_gear(
        mock_gear, {}
    )
    
    # Verify the result exists and deploy_datetime was converted to string
    assert result is not None
    assert len(result.traps) == 1
    trap = result.traps[0]
    assert trap.deploy_datetime_utc == "1672531200"  # Should be string conversion of custom object
