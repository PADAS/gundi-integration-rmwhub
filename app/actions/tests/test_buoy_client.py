import pytest
import httpx
import asyncio
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch
from urllib.parse import urljoin

from app.actions.buoy.client import BuoyClient
from app.actions.buoy.types import BuoyGear, BuoyDevice, DeviceLocation


class TestBuoyClient:
    """Test suite for BuoyClient class."""
    
    @pytest.fixture
    def client(self):
        """Create a BuoyClient instance for testing."""
        return BuoyClient(
            er_token="test_token",
            er_site="https://example.com",
            default_timeout=10.0,
            connect_timeout=2.0,
            read_timeout=5.0
        )
    
    @pytest.fixture
    def sample_gear_data(self):
        """Sample gear data for testing."""
        return {
            "id": "12345678-1234-1234-1234-123456789012",
            "display_id": "GEAR001",
            "name": "Test Gear",
            "status": "deployed",
            "last_updated": "2023-10-01T12:00:00",
            "type": "buoy",
            "manufacturer": "Test Manufacturer",
            "devices": [
                {
                    "device_id": "100a5ed3-e26c-4904-8a6a-f9cd57343a37",
                    "label": "Device 1",
                    "location": {
                        "latitude": 45.0,
                        "longitude": -120.0
                    },
                    "last_updated": "2023-10-01T12:00:00",
                    "last_deployed": "2023-09-15T08:00:00"
                },
                {
                    "device_id": "2962e425-fcf8-4506-bd9f-672943c29196",
                    "label": "Device 2",
                    "location": {
                        "latitude": 46.0,
                        "longitude": -121.0
                    },
                    "last_updated": "2023-10-01T11:30:00",
                    "last_deployed": None
                }
            ]
        }
    
    @pytest.fixture
    def api_response(self, sample_gear_data):
        """Sample API response for testing."""
        return {
            "data": {
                "results": [sample_gear_data],
                "next": None
            }
        }
    
    def test_init_basic(self):
        """Test basic initialization of BuoyClient."""
        client = BuoyClient(er_token="token", er_site="https://example.com")
        
        assert client.er_token == "token"
        assert client.er_site == "https://example.com/"
        assert client.headers["Authorization"] == "Bearer token"
        assert client.headers["Content-Type"] == "application/json"
        assert isinstance(client.default_timeout, httpx.Timeout)
    
    def test_init_with_custom_timeouts(self):
        """Test initialization with custom timeout values."""
        client = BuoyClient(
            er_token="token",
            er_site="https://example.com",
            default_timeout=60.0,
            connect_timeout=10.0,
            read_timeout=45.0
        )
        
        # httpx.Timeout object stores values internally, verify it was created with custom values
        assert isinstance(client.default_timeout, httpx.Timeout)
    
    def test_sanitize_base_url_add_https(self):
        """Test URL sanitization when no scheme is provided."""
        client = BuoyClient(er_token="token", er_site="example.com")
        assert client.er_site == "https://example.com/"
    
    def test_sanitize_base_url_add_trailing_slash(self):
        """Test URL sanitization adds trailing slash."""
        client = BuoyClient(er_token="token", er_site="https://example.com")
        assert client.er_site == "https://example.com/"
    
    def test_sanitize_base_url_preserve_http(self):
        """Test URL sanitization preserves http scheme."""
        client = BuoyClient(er_token="token", er_site="http://example.com")
        assert client.er_site == "http://example.com/"
    
    def test_sanitize_base_url_empty_raises_error(self):
        """Test that empty URL raises ValueError."""
        with pytest.raises(ValueError, match="Base URL cannot be empty"):
            BuoyClient(er_token="token", er_site="")
    
    def test_sanitize_base_url_invalid_format_raises_error(self):
        """Test that invalid URL format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid URL format"):
            BuoyClient(er_token="token", er_site="https://")
    
    def test_create_timeout_default(self):
        """Test create_timeout with default values."""
        timeout = BuoyClient.create_timeout()
        
        # Verify it's an httpx.Timeout object
        assert isinstance(timeout, httpx.Timeout)
    
    def test_create_timeout_custom(self):
        """Test create_timeout with custom values."""
        timeout = BuoyClient.create_timeout(
            timeout=60.0,
            connect=10.0,
            read=40.0,
            write=15.0,
            pool=8.0
        )
        
        # Verify it's an httpx.Timeout object with custom values
        assert isinstance(timeout, httpx.Timeout)
    
    def test_parse_gear_complete_data(self, client, sample_gear_data):
        """Test parsing gear data with complete information."""
        gear = client._parse_gear(sample_gear_data)
        
        assert isinstance(gear, BuoyGear)
        assert str(gear.id) == sample_gear_data["id"]
        assert gear.display_id == sample_gear_data["display_id"]
        assert gear.name == sample_gear_data["name"]
        assert gear.status == sample_gear_data["status"]
        assert gear.type == sample_gear_data["type"]
        assert gear.manufacturer == sample_gear_data["manufacturer"]
        assert len(gear.devices) == 2
        
        # Check first device
        device1 = gear.devices[0]
        assert device1.device_id == "DEV001"
        assert device1.label == "Device 1"
        assert device1.location.latitude == 45.0
        assert device1.location.longitude == -120.0
        assert device1.last_deployed is not None
        
        # Check second device (no last_deployed)
        device2 = gear.devices[1]
        assert device2.device_id == "DEV002"
        assert device2.last_deployed is None
    
    def test_parse_gear_minimal_data(self, client):
        """Test parsing gear data with minimal information."""
        minimal_data = {
            "id": "12345678-1234-1234-1234-123456789012",
            "display_id": "GEAR002",
            "devices": []
        }
        
        gear = client._parse_gear(minimal_data)
        
        assert str(gear.id) == minimal_data["id"]
        assert gear.display_id == "GEAR002"
        assert gear.name == "GEAR002"  # Falls back to display_id when name is missing
        assert gear.status == ""  # Default empty string
        assert gear.type == ""  # Default empty string
        assert gear.manufacturer == ""  # Default empty string
        assert len(gear.devices) == 0
    
    def test_parse_gear_with_name_fallback(self, client):
        """Test parsing gear data where name falls back to display_id."""
        data = {
            "id": "12345678-1234-1234-1234-123456789012",
            "display_id": "GEAR003",
            "devices": []
        }
        
        gear = client._parse_gear(data)
        assert gear.name == "GEAR003"  # Should use display_id when name is missing
    
    @pytest.mark.asyncio
    async def test_iter_gears_success_single_page(self, client, api_response):
        """Test successful iteration over gears with a single page."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = api_response
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client
            
            gears = []
            async for gear in client.iter_gears():
                gears.append(gear)
            
            assert len(gears) == 1
            assert isinstance(gears[0], BuoyGear)
            assert gears[0].display_id == "GEAR001"
            
            # Verify the request was made correctly
            mock_client.get.assert_called_once_with(
                urljoin(client.er_site, "gear/"),
                headers=client.headers,
                params=None
            )
    
    @pytest.mark.asyncio
    async def test_iter_gears_success_multiple_pages(self, client, sample_gear_data):
        """Test successful iteration over gears with multiple pages."""
        # First page response
        first_response = {
            "data": {
                "results": [sample_gear_data],
                "next": "https://example.com/gear/?page=2"
            }
        }
        
        # Second page response
        second_gear_data = {**sample_gear_data, "id": "87654321-4321-4321-4321-210987654321", "display_id": "GEAR002"}
        second_response = {
            "data": {
                "results": [second_gear_data],
                "next": None
            }
        }
        
        mock_response_1 = Mock()
        mock_response_1.status_code = 200
        mock_response_1.json.return_value = first_response
        
        mock_response_2 = Mock()
        mock_response_2.status_code = 200
        mock_response_2.json.return_value = second_response
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.side_effect = [mock_response_1, mock_response_2]
            mock_client_class.return_value = mock_client
            
            gears = []
            async for gear in client.iter_gears():
                gears.append(gear)
            
            assert len(gears) == 2
            assert gears[0].display_id == "GEAR001"
            assert gears[1].display_id == "GEAR002"
            
            # Verify two requests were made
            assert mock_client.get.call_count == 2
    
    @pytest.mark.asyncio
    async def test_iter_gears_with_params(self, client, api_response):
        """Test iteration with custom parameters."""
        params = {"status": "deployed"}
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = api_response
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client
            
            gears = []
            async for gear in client.iter_gears(params=params):
                gears.append(gear)
            
            # Verify params were passed in the first request
            mock_client.get.assert_called_once_with(
                urljoin(client.er_site, "gear/"),
                headers=client.headers,
                params=params
            )
    
    @pytest.mark.asyncio
    async def test_iter_gears_with_custom_timeout(self, client, api_response):
        """Test iteration with custom timeout."""
        custom_timeout = httpx.Timeout(timeout=60.0, connect=10.0)
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = api_response
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client
            
            gears = []
            async for gear in client.iter_gears(timeout=custom_timeout):
                gears.append(gear)
            
            # Verify the custom timeout was used
            mock_client_class.assert_called_once_with(timeout=custom_timeout)
    
    @pytest.mark.asyncio
    async def test_iter_gears_http_error(self, client):
        """Test iteration when HTTP request fails."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client
            
            gears = []
            async for gear in client.iter_gears():
                gears.append(gear)
            
            # Should return empty list on error
            assert len(gears) == 0
    
    @pytest.mark.asyncio
    async def test_iter_gears_missing_data_field(self, client):
        """Test iteration when response is missing 'data' field."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"error": "Invalid response"}
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client
            
            gears = []
            async for gear in client.iter_gears():
                gears.append(gear)
            
            # Should return empty list on malformed response
            assert len(gears) == 0
    
    @pytest.mark.asyncio
    async def test_iter_gears_missing_results_field(self, client):
        """Test iteration when response is missing 'results' field."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": {"next": None}}
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client
            
            gears = []
            async for gear in client.iter_gears():
                gears.append(gear)
            
            # Should return empty list on malformed response
            assert len(gears) == 0
    
    @pytest.mark.asyncio
    async def test_iter_gears_empty_results(self, client):
        """Test iteration when results list is empty."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "results": [],
                "next": None
            }
        }
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client
            
            gears = []
            async for gear in client.iter_gears():
                gears.append(gear)
            
            # Should return empty list when no results
            assert len(gears) == 0
    
    def test_parse_gear_with_empty_devices_list(self, client):
        """Test parsing gear with empty devices list."""
        data = {
            "id": "12345678-1234-1234-1234-123456789012",
            "display_id": "GEAR001",
            "name": "Test Gear",
            "status": "deployed",
            "last_updated": "2023-10-01T12:00:00",
            "type": "buoy",
            "manufacturer": "Test Manufacturer",
            "devices": []
        }
        
        gear = client._parse_gear(data)
        assert len(gear.devices) == 0
        assert isinstance(gear.devices, list)
    
    def test_parse_gear_device_without_last_deployed(self, client):
        """Test parsing device data without last_deployed field."""
        data = {
            "id": "12345678-1234-1234-1234-123456789012",
            "display_id": "GEAR001",
            "devices": [{
                "device_id": "07a3e45b-b8b5-45ca-a19e-155b85ef6591",
                "label": "Device 1",
                "location": {"latitude": 45.0, "longitude": -120.0},
                "last_updated": "2023-10-01T12:00:00"
                # last_deployed is missing
            }]
        }
        
        gear = client._parse_gear(data)
        assert len(gear.devices) == 1
        assert gear.devices[0].last_deployed is None
    
    def test_parse_gear_device_with_missing_location_fields(self, client):
        """Test parsing device with missing location fields (defaults to 0.0)."""
        data = {
            "id": "12345678-1234-1234-1234-123456789012",
            "display_id": "GEAR001",
            "devices": [{
                "device_id": "8ab2da67-1808-438f-a138-9a108bd40d14",
                "label": "Device 1",
                "location": {},  # Empty location
                "last_updated": "2023-10-01T12:00:00"
            }]
        }
        
        gear = client._parse_gear(data)
        device = gear.devices[0]
        assert device.location.latitude == 0.0
        assert device.location.longitude == 0.0
    
    def test_parse_gear_device_with_default_values(self, client):
        """Test parsing device with missing fields that get default values."""
        data = {
            "id": "12345678-1234-1234-1234-123456789012",
            "display_id": "GEAR001",
            "devices": [{
                # Missing device_id, label, location
                "last_updated": "2023-10-01T12:00:00"
            }]
        }
        
        gear = client._parse_gear(data)
        device = gear.devices[0]
        assert device.device_id == ""
        assert device.label == ""
        assert device.location.latitude == 0.0
        assert device.location.longitude == 0.0
    
    @pytest.mark.asyncio
    async def test_iter_gears_default_timeout_used(self, client, api_response):
        """Test that default timeout is used when no custom timeout provided."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = api_response
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client
            
            gears = []
            async for gear in client.iter_gears():
                gears.append(gear)
            
            # Verify the default timeout was used
            mock_client_class.assert_called_once_with(timeout=client.default_timeout)

    @pytest.mark.asyncio
    async def test_get_all_gears_success(self, client, sample_gear_data):
        """Test successful retrieval of all gears (deployed and hauled)."""
        deployed_gear_data = {**sample_gear_data, "status": "deployed", "id": "12345678-1234-1234-1234-123456789012"}
        hauled_gear_data = {**sample_gear_data, "status": "hauled", "id": "87654321-4321-4321-4321-210987654321", "display_id": "GEAR002"}
        
        deployed_response = {
            "data": {
                "results": [deployed_gear_data],
                "next": None
            }
        }
        
        hauled_response = {
            "data": {
                "results": [hauled_gear_data],
                "next": None
            }
        }
        
        mock_deployed_response = Mock()
        mock_deployed_response.status_code = 200
        mock_deployed_response.json.return_value = deployed_response
        
        mock_hauled_response = Mock()
        mock_hauled_response.status_code = 200
        mock_hauled_response.json.return_value = hauled_response
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.side_effect = [mock_deployed_response, mock_hauled_response]
            mock_client_class.return_value = mock_client
            
            gears = await client.get_all_gears()
            
            assert len(gears) == 2
            assert gears[0].status == "deployed"
            assert gears[1].status == "hauled"
            
            # Verify two requests were made (one for deployed, one for hauled)
            assert mock_client.get.call_count == 2
            
            # Verify the correct parameters were used
            calls = mock_client.get.call_args_list
            assert calls[0][1]['params'] == {"state": "deployed"}
            assert calls[1][1]['params'] == {"state": "hauled"}

    @pytest.mark.asyncio
    async def test_get_all_gears_with_timeout(self, client, sample_gear_data):
        """Test get_all_gears with custom timeout."""
        custom_timeout = httpx.Timeout(timeout=60.0)
        
        api_response = {
            "data": {
                "results": [sample_gear_data],
                "next": None
            }
        }
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = api_response
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client
            
            gears = await client.get_all_gears(timeout=custom_timeout)
            
            assert len(gears) == 2  # Both deployed and hauled calls return same gear
            
            # Verify custom timeout was used in both calls
            assert mock_client_class.call_count == 2
            for call in mock_client_class.call_args_list:
                assert call[1]['timeout'] == custom_timeout

    @pytest.mark.asyncio
    async def test_get_all_gears_empty_results(self, client):
        """Test get_all_gears when no gears are returned."""
        empty_response = {
            "data": {
                "results": [],
                "next": None
            }
        }
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = empty_response
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client
            
            gears = await client.get_all_gears()
            
            assert len(gears) == 0
            assert isinstance(gears, list)

    @pytest.mark.asyncio
    async def test_get_all_gears_error_handling(self, client):
        """Test get_all_gears when API calls fail."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_client.get.return_value = mock_response
            mock_client_class.return_value = mock_client
            
            gears = await client.get_all_gears()
            
            # Should return empty list when both calls fail
            assert len(gears) == 0
            assert isinstance(gears, list)

    def test_parse_gear_device_defaults_mfr_device_id_to_empty_string(self, client):
        """Test that missing mfr_device_id defaults to empty string when parsing device data."""        
        data = {
            "id": "12345678-1234-1234-1234-123456789012",
            "display_id": "GEAR001",
            "devices": [{
                "device_id": "d5a95ce7-a7f5-4afc-87fe-b6f7e3d6e063",
                "label": "Device 1",
                "location": {"latitude": 45.0, "longitude": -120.0},
                "last_updated": "2023-10-01T12:00:00"
                # mfr_device_id is missing, should default to empty string
            }]
        }
        
        gear = client._parse_gear(data)
        assert len(gear.devices) == 1
        assert gear.devices[0].mfr_device_id == ""  # Should default to empty string

    def test_parse_gear_device_defaults_mfr_device_id_to_empty_string(self, client):
        """Test that missing mfr_device_id defaults to empty string when parsing device data."""
        data = {
            "id": "12345678-1234-1234-1234-123456789012",
            "display_id": "GEAR001",
            "devices": []
            # manufacturer field is missing
        }
        
        gear = client._parse_gear(data)
        assert gear.manufacturer == ""  # Should default to empty string

    def test_parse_gear_device_with_none_last_deployed(self, client):
        """Test parsing device with None last_deployed value."""
        data = {
            "id": "12345678-1234-1234-1234-123456789012",
            "display_id": "GEAR001",
            "devices": [{
                "device_id": "d84b5add-0764-48c1-94d4-59a6abb60bfd",
                "label": "Device 1",
                "location": {"latitude": 45.0, "longitude": -120.0},
                "last_updated": "2023-10-01T12:00:00",
                "last_deployed": None  # Explicitly None
            }]
        }
        
        gear = client._parse_gear(data)
        assert len(gear.devices) == 1
        assert gear.devices[0].last_deployed is None
