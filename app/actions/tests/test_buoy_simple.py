"""
Simple tests for BuoyClient to improve coverage
"""
import pytest
from unittest.mock import AsyncMock, Mock
import httpx
from app.actions.buoy.client import BuoyClient


@pytest.fixture
def buoy_client():
    """Fixture for BuoyClient instance"""
    return BuoyClient(
        er_token="test_token",
        er_site="https://test.earthranger.com/api/v1.0/"
    )


@pytest.mark.asyncio
async def test_get_er_gears_success(buoy_client, mocker):
    """Test successful gear retrieval"""
    mock_response_data = {
        "data": {
            "results": [
                {
                    "id": "123e4567-e89b-12d3-a456-426614174000",
                    "display_id": "test_gear_001",
                    "name": "Test Gear",
                    "status": "active",
                    "last_updated": "2023-01-01T00:00:00+00:00",
                    "devices": [],
                    "type": "fishing_gear",
                    "manufacturer": "Test Mfg",
                    "additional": {"subject_subtype": "test_subtype"},
                    "location": {"type": "Point", "coordinates": [-122.0, 37.0]},
                    "is_active": True,
                    "external_id": "ext_123"
                }
            ],
            "next": None
        }
    }
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_response_data
    
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await buoy_client.get_er_gears()
    
    assert len(result) == 1
    assert result[0].display_id == "test_gear_001"
    assert result[0].name == "Test Gear"


@pytest.mark.asyncio
async def test_get_er_gears_http_error(buoy_client, mocker):
    """Test gear retrieval with HTTP error"""
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await buoy_client.get_er_gears()
    
    assert result == []


@pytest.mark.asyncio
async def test_get_er_gears_empty_results(buoy_client, mocker):
    """Test gear retrieval with empty results"""
    mock_response_data = {
        "data": {
            "results": [],
            "next": None
        }
    }
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_response_data
    
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await buoy_client.get_er_gears()
    
    assert result == []


@pytest.mark.asyncio
async def test_get_er_gears_missing_data_field(buoy_client, mocker):
    """Test gear retrieval with missing data field"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"invalid": "response"}
    
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await buoy_client.get_er_gears()
    
    assert result == []


@pytest.mark.asyncio
async def test_get_er_gears_with_pagination(buoy_client, mocker):
    """Test gear retrieval with pagination"""
    # First page
    first_page = {
        "data": {
            "results": [
                {
                    "id": "123e4567-e89b-12d3-a456-426614174000",
                    "display_id": "gear_1",
                    "name": "Gear 1",
                    "status": "active",
                    "last_updated": "2023-01-01T00:00:00+00:00",
                    "devices": [],
                    "type": "fishing_gear",
                    "manufacturer": "Test Mfg"
                }
            ],
            "next": "https://test.earthranger.com/api/v1.0/gear/?page=2"
        }
    }
    
    # Second page
    second_page = {
        "data": {
            "results": [
                {
                    "id": "456e7890-e89b-12d3-a456-426614174000",
                    "display_id": "gear_2",
                    "name": "Gear 2",
                    "status": "active",
                    "last_updated": "2023-01-01T00:00:00+00:00",
                    "devices": [],
                    "type": "fishing_gear",
                    "manufacturer": "Test Mfg"
                }
            ],
            "next": None
        }
    }
    
    mock_response_1 = Mock()
    mock_response_1.status_code = 200
    mock_response_1.json.return_value = first_page
    
    mock_response_2 = Mock()
    mock_response_2.status_code = 200
    mock_response_2.json.return_value = second_page
    
    mock_client = AsyncMock()
    mock_client.get.side_effect = [mock_response_1, mock_response_2]
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await buoy_client.get_er_gears()
    
    assert len(result) == 2
    assert result[0].display_id == "gear_1"
    assert result[1].display_id == "gear_2"
    assert mock_client.get.call_count == 2


def test_headers_property(buoy_client):
    """Test headers property"""
    headers = buoy_client.headers
    
    expected_headers = {
        "Authorization": "Bearer test_token",
        "Content-Type": "application/json"
    }
    assert headers == expected_headers


@pytest.mark.asyncio
async def test_get_er_gears_missing_results_field(buoy_client, mocker):
    """Test gear retrieval with missing results field"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": {"invalid": "structure"}}
    
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await buoy_client.get_er_gears()
    
    assert result == []


@pytest.mark.asyncio
async def test_create_gear_success(buoy_client, mocker):
    """Test successful gear creation"""
    gear_data = {
        "display_id": "new_gear_001",
        "name": "New Test Gear",
        "type": "fishing_gear"
    }
    
    mock_response_data = {
        "id": "123e4567-e89b-12d3-a456-426614174000",
        "display_id": "new_gear_001",
        "name": "New Test Gear",
        "status": "active",
        "last_updated": "2023-01-01T00:00:00+00:00",
        "devices": [],
        "type": "fishing_gear",
        "manufacturer": "Test Mfg"
    }
    
    mock_response = Mock()
    mock_response.status_code = 201
    mock_response.json.return_value = mock_response_data
    
    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await buoy_client.create_gear(gear_data)
    
    assert result is not None
    assert result.display_id == "new_gear_001"
    assert result.name == "New Test Gear"
    
    mock_client.post.assert_called_once_with(
        "https://test.earthranger.com/api/v1.0/gear/",
        headers=buoy_client.headers,
        json=gear_data
    )


@pytest.mark.asyncio
async def test_create_gear_http_error(buoy_client, mocker):
    """Test gear creation with HTTP error"""
    gear_data = {"display_id": "new_gear_001"}
    
    mock_response = Mock()
    mock_response.status_code = 400
    mock_response.text = "Bad Request"
    
    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await buoy_client.create_gear(gear_data)
    
    assert result is None


@pytest.mark.asyncio
async def test_update_gear_success(buoy_client, mocker):
    """Test successful gear update"""
    gear_id = "123e4567-e89b-12d3-a456-426614174000"
    gear_data = {"name": "Updated Gear Name"}
    
    mock_response_data = {
        "id": gear_id,
        "display_id": "test_gear_001",
        "name": "Updated Gear Name",
        "status": "active",
        "last_updated": "2023-01-01T00:00:00+00:00",
        "devices": [],
        "type": "fishing_gear",
        "manufacturer": "Test Mfg"
    }
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_response_data
    
    mock_client = AsyncMock()
    mock_client.patch.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await buoy_client.update_gear(gear_id, gear_data)
    
    assert result is not None
    assert result.name == "Updated Gear Name"
    
    mock_client.patch.assert_called_once_with(
        f"https://test.earthranger.com/api/v1.0/gear/{gear_id}/",
        headers=buoy_client.headers,
        json=gear_data
    )


@pytest.mark.asyncio
async def test_update_gear_http_error(buoy_client, mocker):
    """Test gear update with HTTP error"""
    gear_id = "123e4567-e89b-12d3-a456-426614174000"
    gear_data = {"name": "Updated Name"}
    
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.text = "Not Found"
    
    mock_client = AsyncMock()
    mock_client.patch.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await buoy_client.update_gear(gear_id, gear_data)
    
    assert result is None


@pytest.mark.asyncio
async def test_get_er_gears_no_items_warning(buoy_client, mocker):
    """Test gear retrieval warning when no items found"""
    mock_response_data = {
        "data": {
            "results": [],
            "next": None
        }
    }
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_response_data
    
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    mock_logger = mocker.patch('app.actions.buoy.client.logger')
    
    result = await buoy_client.get_er_gears()
    
    assert result == []
    mock_logger.warning.assert_called_once_with("No gears found")


@pytest.mark.asyncio
async def test_get_er_gears_with_devices(buoy_client, mocker):
    """Test gear retrieval parsing devices"""
    mock_response_data = {
        "data": {
            "results": [
                {
                    "id": "123e4567-e89b-12d3-a456-426614174000",
                    "display_id": "gear_with_devices",
                    "name": "Gear with Devices",
                    "status": "active",
                    "last_updated": "2023-01-01T00:00:00+00:00",
                    "type": "fishing_gear",
                    "manufacturer": "Test Mfg",
                    "devices": [
                        {
                            "device_id": "device_001",
                            "label": "Device 1",
                            "location": {
                                "latitude": 37.5,
                                "longitude": -122.5
                            },
                            "last_updated": "2023-01-01T00:00:00+00:00",
                            "last_deployed": "2022-12-01T00:00:00+00:00"
                        },
                        {
                            "device_id": "device_002",
                            "label": "Device 2",
                            "location": {
                                "latitude": 37.6,
                                "longitude": -122.6
                            },
                            "last_updated": "2023-01-01T00:00:00+00:00",
                            "last_deployed": None  # Test None case
                        }
                    ]
                }
            ],
            "next": None
        }
    }
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_response_data
    
    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await buoy_client.get_er_gears()
    
    assert len(result) == 1
    gear = result[0]
    assert gear.display_id == "gear_with_devices"
    assert len(gear.devices) == 2
    
    # Test first device
    device1 = gear.devices[0]
    assert device1.device_id == "device_001"
    assert device1.label == "Device 1"
    assert device1.location.latitude == 37.5
    assert device1.location.longitude == -122.5
    assert device1.last_deployed is not None
    
    # Test second device (with None last_deployed)
    device2 = gear.devices[1]
    assert device2.device_id == "device_002"
    assert device2.label == "Device 2"
    assert device2.location.latitude == 37.6
    assert device2.location.longitude == -122.6
    assert device2.last_deployed is None
