"""
Tests for RmwHubClient functionality
"""
import pytest
from unittest.mock import AsyncMock, Mock
import httpx
import pytz
from datetime import datetime
from app.actions.rmwhub.client import RmwHubClient
from app.actions.rmwhub.types import GearSet, Trap


@pytest.fixture
def rmw_client():
    """Fixture for RmwHubClient instance"""
    return RmwHubClient(
        api_key="test_api_key",
        rmw_url="https://test.rmwhub.com/api"
    )


@pytest.fixture
def sample_gearset():
    """Fixture for sample GearSet"""
    return GearSet(
        id="test_set_001",
        vessel_id="vessel_001",
        deployment_type="normal",
        trawl_path="path_001",
        traps_in_set=2,
        when_updated_utc="2023-01-01T00:00:00Z",
        traps=[
            Trap(
                id="trap_001",
                sequence=1,
                latitude=37.0,
                longitude=-122.0,
                deploy_datetime_utc="2023-01-01T00:00:00Z",
                status="deployed",
                accuracy="high",
                is_on_end=False
            ),
            Trap(
                id="trap_002", 
                sequence=2,
                latitude=37.1,
                longitude=-122.1,
                deploy_datetime_utc="2023-01-01T01:00:00Z",
                status="deployed",
                accuracy="high",
                is_on_end=True
            )
        ]
    )


@pytest.mark.asyncio
async def test_search_hub_success(rmw_client, mocker):
    """Test successful search_hub call"""
    start_datetime = datetime(2023, 1, 1, tzinfo=pytz.UTC)
    expected_response = '{"sets": [{"id": "test_set"}]}'
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = expected_response
    
    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await rmw_client.search_hub(start_datetime)
    
    assert result == expected_response
    
    # Verify the API call
    expected_data = {
        "format_version": 0.1,
        "api_key": "test_api_key",
        "max_sets": 10000,
        "start_datetime_utc": "2023-01-01T00:00:00+00:00"
    }
    
    mock_client.post.assert_called_once_with(
        "https://test.rmwhub.com/api/search_hub/",
        headers=RmwHubClient.HEADERS,
        json=expected_data
    )


@pytest.mark.asyncio
async def test_search_hub_with_status(rmw_client, mocker):
    """Test search_hub call with status parameter"""
    start_datetime = datetime(2023, 1, 1, tzinfo=pytz.UTC)
    expected_response = '{"sets": []}'
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = expected_response
    
    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await rmw_client.search_hub(start_datetime, status=True)
    
    assert result == expected_response
    
    # Verify status was included in the request
    call_args = mock_client.post.call_args
    request_data = call_args[1]['json']
    assert 'status' in request_data
    assert request_data['status'] is True


@pytest.mark.asyncio
async def test_search_hub_http_error(rmw_client, mocker):
    """Test search_hub with HTTP error"""
    start_datetime = datetime(2023, 1, 1, tzinfo=pytz.UTC)
    
    mock_response = Mock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    
    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    # Should still return the response text even on error
    result = await rmw_client.search_hub(start_datetime)
    
    assert result == "Internal Server Error"


@pytest.mark.asyncio
async def test_upload_data_success(rmw_client, sample_gearset, mocker):
    """Test successful upload_data call"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b'{"success": true}'
    
    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await rmw_client.upload_data([sample_gearset])
    
    assert result == mock_response
    assert result.status_code == 200
    
    # Verify the API call structure
    call_args = mock_client.post.call_args
    assert call_args[0][0] == "https://test.rmwhub.com/api/upload_deployments/"
    assert call_args[1]['headers'] == RmwHubClient.HEADERS
    
    # Verify data transformation
    request_data = call_args[1]['json']
    assert 'format_version' in request_data
    assert request_data['format_version'] == 0
    assert 'api_key' in request_data
    assert request_data['api_key'] == "test_api_key"
    assert 'sets' in request_data
    assert len(request_data['sets']) == 1
    
    # Verify set_id transformation (id -> set_id)
    set_data = request_data['sets'][0]
    assert 'set_id' in set_data
    assert set_data['set_id'] == "test_set_001"
    assert 'id' not in set_data
    
    # Verify trap_id transformation (id -> trap_id)
    for trap in set_data['traps']:
        assert 'trap_id' in trap
        assert 'id' not in trap
        assert 'release_type' in trap  # Should be added as empty string


@pytest.mark.asyncio
async def test_upload_data_http_error(rmw_client, sample_gearset, mocker):
    """Test upload_data with HTTP error"""
    mock_response = Mock()
    mock_response.status_code = 400
    mock_response.content = b'{"error": "Bad Request"}'
    
    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await rmw_client.upload_data([sample_gearset])
    
    assert result == mock_response
    assert result.status_code == 400


@pytest.mark.asyncio
async def test_upload_data_empty_list(rmw_client, mocker):
    """Test upload_data with empty gear list"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b'{"success": true}'
    
    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await rmw_client.upload_data([])
    
    assert result == mock_response
    
    # Verify empty sets array
    call_args = mock_client.post.call_args
    request_data = call_args[1]['json']
    assert request_data['sets'] == []


@pytest.mark.asyncio 
async def test_upload_data_with_release_type(rmw_client, mocker):
    """Test upload_data preserves existing release_type"""
    gearset_with_release_type = GearSet(
        id="test_set_002",
        vessel_id="vessel_002",
        deployment_type="normal",
        trawl_path="path_002",
        traps_in_set=1,
        when_updated_utc="2023-01-01T00:00:00Z",
        traps=[
            Trap(
                id="trap_003",
                sequence=1,
                latitude=37.0,
                longitude=-122.0,
                deploy_datetime_utc="2023-01-01T00:00:00Z",
                status="deployed",
                accuracy="high",
                is_on_end=False,
                release_type="acoustic"  # Existing release_type
            )
        ]
    )
    
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b'{"success": true}'
    
    mock_client = AsyncMock()
    mock_client.post.return_value = mock_response
    mock_async_client = AsyncMock()
    mock_async_client.__aenter__.return_value = mock_client
    
    mocker.patch('httpx.AsyncClient', return_value=mock_async_client)
    
    result = await rmw_client.upload_data([gearset_with_release_type])
    
    # Verify existing release_type is preserved
    call_args = mock_client.post.call_args
    request_data = call_args[1]['json']
    trap_data = request_data['sets'][0]['traps'][0]
    assert trap_data['release_type'] == "acoustic"


def test_headers_constant():
    """Test HEADERS constant is properly defined"""
    assert hasattr(RmwHubClient, 'HEADERS')
    assert RmwHubClient.HEADERS == {"accept": "application/json", "Content-Type": "application/json"}


def test_client_initialization():
    """Test RmwHubClient initialization"""
    client = RmwHubClient("test_key", "https://test.url")
    
    assert client.api_key == "test_key"
    assert client.rmw_url == "https://test.url"
