"""
Additional tests for RmwHubAdapter to achieve higher coverage
"""
import pytest
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime
import pytz
import json
import httpx
import uuid
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
async def test_search_own_json_decode_error(rmw_adapter, mocker):
    """Test search_own with JSON decode error"""
    # Mock httpx response with invalid JSON
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.side_effect = json.JSONDecodeError("test error", "test", 0)
    
    mock_client = Mock()
    mock_client.post = AsyncMock(return_value=mock_response)
    
    with patch('httpx.AsyncClient') as mock_async_client:
        mock_async_client.return_value.__aenter__.return_value = mock_client
        mock_logger = mocker.patch('app.actions.rmwhub.adapter.logger')
        
        result = await rmw_adapter.search_own()
        
        assert result == []
        mock_logger.error.assert_called()


@pytest.mark.asyncio
async def test_search_own_missing_sets_key(rmw_adapter, mocker):
    """Test search_own with missing 'sets' key in response"""
    # Mock httpx response with JSON missing 'sets' key
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"other_key": []}
    
    mock_client = Mock()
    mock_client.post = AsyncMock(return_value=mock_response)
    
    with patch('httpx.AsyncClient') as mock_async_client:
        mock_async_client.return_value.__aenter__.return_value = mock_client
        mock_logger = mocker.patch('app.actions.rmwhub.adapter.logger')
        
        result = await rmw_adapter.search_own()
        
        assert result == []
        mock_logger.error.assert_called()


@pytest.mark.asyncio
async def test_search_own_non_200_status(rmw_adapter, mocker):
    """Test search_own with non-200 HTTP status"""
    # Mock httpx response with error status
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    
    mock_client = Mock()
    mock_client.post = AsyncMock(return_value=mock_response)
    
    with patch('httpx.AsyncClient') as mock_async_client:
        mock_async_client.return_value.__aenter__.return_value = mock_client
        mock_logger = mocker.patch('app.actions.rmwhub.adapter.logger')
        
        result = await rmw_adapter.search_own()
        
        assert result == []
        mock_logger.error.assert_called()


def test_convert_to_sets_missing_sets_key(rmw_adapter, mocker):
    """Test convert_to_sets with missing 'sets' key"""
    mock_logger = mocker.patch('app.actions.rmwhub.adapter.logger')
    
    response_json = {"other_key": []}
    result = rmw_adapter.convert_to_sets(response_json)
    
    assert result == []
    mock_logger.error.assert_called()


def test_convert_to_sets_valid_data(rmw_adapter):
    """Test convert_to_sets with valid data"""
    response_json = {
        "sets": [
            {
                "vessel_id": "vessel_1",
                "set_id": "set_1",
                "deployment_type": "lobster",
                "traps_in_set": 5,
                "trawl_path": "some_path",  # Must be string, not list
                "when_updated_utc": "2023-01-01T00:00:00Z",
                "traps": [
                    {
                        "trap_id": "trap_1",
                        "sequence": 1,
                        "latitude": 42.0,
                        "longitude": -70.0,
                        "deploy_datetime_utc": "2023-01-01T00:00:00Z",
                        "surface_datetime_utc": None,
                        "retrieved_datetime_utc": None,
                        "status": "deployed",
                        "accuracy": "high",
                        "release_type": "acoustic",
                        "is_on_end": True
                    }
                ]
            }
        ]
    }
    
    result = rmw_adapter.convert_to_sets(response_json)
    
    assert len(result) == 1
    assert result[0].id == "set_1"
    assert result[0].vessel_id == "vessel_1"
    assert len(result[0].traps) == 1
    assert result[0].traps[0].id == "trap_1"


def test_create_traps_gearsets_mapping_key(rmw_adapter):
    """Test _create_traps_gearsets_mapping_key method"""
    trap_ids = ["trap_3", "trap_1", "trap_2"]
    result = rmw_adapter._create_traps_gearsets_mapping_key(trap_ids)
    
    assert isinstance(result, str)
    assert len(result) == 12  # SHA256 hash truncated to 12 chars
    
    # Test with same IDs in different order should give same result
    trap_ids_reordered = ["trap_1", "trap_2", "trap_3"]
    result2 = rmw_adapter._create_traps_gearsets_mapping_key(trap_ids_reordered)
    assert result == result2


def test_integration_uuid_property(rmw_adapter, a_good_integration):
    """Test integration_uuid property"""
    result = rmw_adapter.integration_uuid
    assert isinstance(result, uuid.UUID)
    assert str(result) == str(a_good_integration.id)


def test_integration_uuid_with_string_id():
    """Test integration_uuid with string ID"""
    uuid_str = "12345678-1234-1234-1234-123456789012"
    adapter = RmwHubAdapter(
        integration_id=uuid_str,
        api_key="test_key",
        rmw_url="https://test.com",
        er_token="test_token",
        er_destination="https://test.earthranger.com/api/v1.0/",
    )
    
    result = adapter.integration_uuid
    assert isinstance(result, uuid.UUID)
    assert str(result) == uuid_str


@pytest.mark.asyncio
async def test_get_er_gears(rmw_adapter, mocker):
    """Test get_er_gears method"""
    start_datetime = datetime.now(pytz.UTC)
    mock_gears = [Mock()]  # Simple mock gear
    
    rmw_adapter.gear_client.get_gears = AsyncMock(return_value=mock_gears)
    
    result = await rmw_adapter.get_er_gears(start_datetime=start_datetime)
    
    assert result == mock_gears
    rmw_adapter.gear_client.get_gears.assert_called_once_with(
        start_datetime=start_datetime, source_type="ropeless_buoy"  # Correct source type
    )


@pytest.mark.asyncio
async def test_search_own_with_parameters(rmw_adapter, mocker):
    """Test search_own with trap_id and status parameters"""
    # Mock successful response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"sets": []}
    
    mock_client = Mock()
    mock_client.post = AsyncMock(return_value=mock_response)
    
    with patch('httpx.AsyncClient') as mock_async_client:
        mock_async_client.return_value.__aenter__.return_value = mock_client
        
        result = await rmw_adapter.search_own(trap_id="trap_123", status="deployed")
        
        assert result == []
        # Verify the correct data was sent in the POST request
        call_args = mock_client.post.call_args
        json_data = call_args[1]['json']
        assert json_data['trap_id'] == "trap_123"
        assert json_data['status'] == "deployed"


@pytest.mark.asyncio
async def test_process_download(rmw_adapter, mocker):
    """Test process_download method"""
    # Mock GearSet with create_observations method
    mock_observations = [{"observation": "data1"}, {"observation": "data2"}]
    
    mock_gearset = Mock()
    mock_gearset.create_observations = AsyncMock(return_value=mock_observations)
    
    rmw_sets = [mock_gearset]
    
    result = await rmw_adapter.process_download(rmw_sets)
    
    assert result == mock_observations
    mock_gearset.create_observations.assert_called_once()


def test_initialization_attributes(rmw_adapter, a_good_integration, a_good_configuration):
    """Test RmwHubAdapter initialization attributes"""
    assert rmw_adapter.integration_id == a_good_integration.id
    assert rmw_adapter.rmw_client.api_key == a_good_configuration.api_key
    assert rmw_adapter.rmw_client.rmw_url == a_good_configuration.rmw_url
    assert hasattr(rmw_adapter, 'er_client')
    assert hasattr(rmw_adapter, 'gear_client')
    assert rmw_adapter.er_subject_name_to_subject_mapping == {}
    assert rmw_adapter.options == {}


def test_initialization_with_options():
    """Test initialization with custom options"""
    custom_options = {"option1": "value1", "option2": "value2"}
    adapter = RmwHubAdapter(
        integration_id="test_id",
        api_key="test_key",
        rmw_url="https://test.com",
        er_token="test_token",
        er_destination="https://test.earthranger.com/api/v1.0/",
        options=custom_options
    )
    
    assert adapter.options == custom_options
