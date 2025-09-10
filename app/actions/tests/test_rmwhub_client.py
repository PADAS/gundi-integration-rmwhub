import pytest
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx
import pytz
from fastapi.encoders import jsonable_encoder

from app.actions.rmwhub.client import RmwHubClient
from app.actions.rmwhub.types import GearSet, Trap


class TestRmwHubClient:
    """Test cases for the RmwHubClient class."""
    
    @pytest.fixture
    def client(self):
        """Fixture for RmwHubClient instance."""
        return RmwHubClient(api_key="test_api_key", rmw_url="https://test.rmwhub.com")
    
    @pytest.fixture
    def sample_datetime(self):
        """Fixture for sample datetime with timezone."""
        return datetime(2023, 9, 15, 14, 30, 0, tzinfo=pytz.timezone('US/Eastern'))
    
    @pytest.fixture
    def sample_trap(self):
        """Fixture for sample Trap."""
        return Trap(
            id="trap_001",
            sequence=1,
            latitude=42.123456,
            longitude=-71.987654,
            deploy_datetime_utc="2023-09-15T14:30:00Z",
            surface_datetime_utc="2023-09-15T16:00:00Z",
            retrieved_datetime_utc="2023-09-15T17:30:00Z",
            status="deployed",
            accuracy="high",
            release_type="manual",
            is_on_end=True
        )
    
    @pytest.fixture
    def sample_gearset(self, sample_trap):
        """Fixture for sample GearSet."""
        return GearSet(
            vessel_id="vessel_001",
            id="gearset_001",
            deployment_type="trawl",
            traps_in_set=1,
            trawl_path="path_001",
            share_with=["partner_001"],
            traps=[sample_trap],
            when_updated_utc="2023-09-15T19:00:00Z"
        )
    
    def test_init(self):
        """Test RmwHubClient initialization."""
        api_key = "test_key_123"
        rmw_url = "https://example.rmwhub.com"
        
        client = RmwHubClient(api_key=api_key, rmw_url=rmw_url)
        
        assert client.api_key == api_key
        assert client.rmw_url == rmw_url
        assert RmwHubClient.HEADERS == {
            "accept": "application/json",
            "Content-Type": "application/json"
        }
    
    @pytest.mark.asyncio
    @patch('httpx.AsyncClient')
    async def test_search_hub_success(self, mock_client_class, client, sample_datetime):
        """Test successful search_hub call."""
        # Mock the response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"format_version": 0.1, "sets": []}'
        
        # Mock the async client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client
        
        # Call the method
        result = await client.search_hub(start_datetime=sample_datetime)
        
        # Assertions
        assert result == '{"format_version": 0.1, "sets": []}'
        
        # Verify the call was made correctly
        expected_data = {
            "format_version": 0.1,
            "api_key": "test_api_key",
            "max_sets": 10000,
            "start_datetime_utc": sample_datetime.astimezone(pytz.utc).isoformat(),
        }
        
        mock_client.post.assert_called_once_with(
            "https://test.rmwhub.com/search_hub/",
            headers=RmwHubClient.HEADERS,
            json=expected_data
        )
    
    @pytest.mark.asyncio
    @patch('httpx.AsyncClient')
    async def test_search_hub_with_status(self, mock_client_class, client, sample_datetime):
        """Test search_hub call with status parameter."""
        # Mock the response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"format_version": 0.1, "sets": []}'
        
        # Mock the async client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client
        
        # Call the method with status
        result = await client.search_hub(start_datetime=sample_datetime, status=True)
        
        # Assertions
        assert result == '{"format_version": 0.1, "sets": []}'
        
        # Verify the call was made correctly with status
        expected_data = {
            "format_version": 0.1,
            "api_key": "test_api_key",
            "max_sets": 10000,
            "start_datetime_utc": sample_datetime.astimezone(pytz.utc).isoformat(),
            "status": True
        }
        
        mock_client.post.assert_called_once_with(
            "https://test.rmwhub.com/search_hub/",
            headers=RmwHubClient.HEADERS,
            json=expected_data
        )
    
    @pytest.mark.asyncio
    @patch('httpx.AsyncClient')
    async def test_search_hub_with_false_status(self, mock_client_class, client, sample_datetime):
        """Test search_hub call with status=False (should not include status in data)."""
        # Mock the response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"format_version": 0.1, "sets": []}'
        
        # Mock the async client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client
        
        # Call the method with status=False
        result = await client.search_hub(start_datetime=sample_datetime, status=False)
        
        # Verify status is not included when False
        expected_data = {
            "format_version": 0.1,
            "api_key": "test_api_key",
            "max_sets": 10000,
            "start_datetime_utc": sample_datetime.astimezone(pytz.utc).isoformat(),
        }
        
        mock_client.post.assert_called_once_with(
            "https://test.rmwhub.com/search_hub/",
            headers=RmwHubClient.HEADERS,
            json=expected_data
        )
    
    @pytest.mark.asyncio
    @patch('httpx.AsyncClient')
    @patch('app.actions.rmwhub.client.logger')
    async def test_search_hub_error_response(self, mock_logger, mock_client_class, client, sample_datetime):
        """Test search_hub with error response."""
        # Mock the response with error
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = '{"error": "Bad request"}'
        
        # Mock the async client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client
        
        # Call the method
        result = await client.search_hub(start_datetime=sample_datetime)
        
        # Assertions
        assert result == '{"error": "Bad request"}'
        
        # Verify error was logged
        mock_logger.error.assert_called_once_with(
            "Failed to download data from RMW Hub API. Error: 400 - {\"error\": \"Bad request\"}"
        )
    
    @pytest.mark.asyncio
    @patch('httpx.AsyncClient')
    async def test_upload_data_success(self, mock_client_class, client, sample_gearset):
        """Test successful upload_data call."""
        # Mock the response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"status": "success"}'
        
        # Mock the async client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client
        
        # Call the method
        result = await client.upload_data([sample_gearset])
        
        # Assertions
        assert result == mock_response
        assert result.status_code == 200
        
        # Verify the call was made
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        
        # Check URL
        assert call_args[0][0] == "https://test.rmwhub.com/upload_deployments/"
        
        # Check headers
        assert call_args[1]["headers"] == RmwHubClient.HEADERS
        
        # Check the JSON data structure
        json_data = call_args[1]["json"]
        assert json_data["format_version"] == 0
        assert json_data["api_key"] == "test_api_key"
        assert len(json_data["sets"]) == 1
        
        # Check that transformations were applied
        set_data = json_data["sets"][0]
        assert "set_id" in set_data
        assert "id" not in set_data
        assert set_data["set_id"] == "gearset_001"
        
        # Check trap transformations
        trap_data = set_data["traps"][0]
        assert "trap_id" in trap_data
        assert "id" not in trap_data
        assert trap_data["trap_id"] == "trap_001"
        assert trap_data["release_type"] == "manual"  # Original value preserved
    
    @pytest.mark.asyncio
    @patch('httpx.AsyncClient')
    async def test_upload_data_with_multiple_gearsets(self, mock_client_class, client, sample_trap):
        """Test upload_data with multiple gearsets."""
        # Create multiple gearsets
        gearset1 = GearSet(
            vessel_id="vessel_001",
            id="gearset_001",
            deployment_type="trawl",
            traps_in_set=1,
            trawl_path="path_001",
            share_with=["partner_001"],
            traps=[sample_trap],
            when_updated_utc="2023-09-15T19:00:00Z"
        )
        
        trap2 = Trap(
            id="trap_002",
            sequence=2,
            latitude=43.0,
            longitude=-72.0,
            deploy_datetime_utc="2023-09-15T15:00:00Z",
            status="retrieved",
            accuracy="medium",
            release_type=None,  # This should be converted to ""
            is_on_end=False
        )
        
        gearset2 = GearSet(
            vessel_id="vessel_002",
            id="gearset_002",
            deployment_type="longline",
            traps_in_set=1,
            trawl_path="path_002",
            share_with=[],
            traps=[trap2],
            when_updated_utc="2023-09-15T20:00:00Z"
        )
        
        # Mock the response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"status": "success"}'
        
        # Mock the async client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client
        
        # Call the method
        result = await client.upload_data([gearset1, gearset2])
        
        # Get the JSON data from the call
        call_args = mock_client.post.call_args
        json_data = call_args[1]["json"]
        
        # Assertions
        assert len(json_data["sets"]) == 2
        
        # Check first gearset
        set1_data = json_data["sets"][0]
        assert set1_data["set_id"] == "gearset_001"
        assert set1_data["traps"][0]["trap_id"] == "trap_001"
        assert set1_data["traps"][0]["release_type"] == "manual"
        
        # Check second gearset
        set2_data = json_data["sets"][1]
        assert set2_data["set_id"] == "gearset_002"
        assert set2_data["traps"][0]["trap_id"] == "trap_002"
        assert set2_data["traps"][0]["release_type"] == ""  # None converted to ""
    
    @pytest.mark.asyncio
    @patch('httpx.AsyncClient')
    async def test_upload_data_with_empty_release_type(self, mock_client_class, client):
        """Test upload_data with trap that has empty release_type."""
        # Create trap with None release_type
        trap = Trap(
            id="trap_empty",
            sequence=1,
            latitude=42.0,
            longitude=-71.0,
            deploy_datetime_utc="2023-09-15T14:30:00Z",
            status="deployed",
            accuracy="high",
            release_type=None,
            is_on_end=True
        )
        
        gearset = GearSet(
            vessel_id="vessel_001",
            id="gearset_001",
            deployment_type="test",
            traps_in_set=1,
            trawl_path="",
            share_with=[],
            traps=[trap],
            when_updated_utc="2023-09-15T19:00:00Z"
        )
        
        # Mock the response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"status": "success"}'
        
        # Mock the async client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client
        
        # Call the method
        await client.upload_data([gearset])
        
        # Check that None release_type was converted to ""
        call_args = mock_client.post.call_args
        json_data = call_args[1]["json"]
        trap_data = json_data["sets"][0]["traps"][0]
        assert trap_data["release_type"] == ""
    
    @pytest.mark.asyncio
    @patch('httpx.AsyncClient')
    @patch('app.actions.rmwhub.client.logger')
    async def test_upload_data_error_response(self, mock_logger, mock_client_class, client, sample_gearset):
        """Test upload_data with error response."""
        # Mock the response with error
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.content = b'{"error": "Internal server error"}'
        
        # Mock the async client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client
        
        # Call the method
        result = await client.upload_data([sample_gearset])
        
        # Assertions
        assert result == mock_response
        assert result.status_code == 500
        
        # Verify error was logged
        mock_logger.error.assert_called_once_with(
            'Failed to upload data to RMW Hub API. Error: 500 - b\'{"error": "Internal server error"}\''
        )
    
    @pytest.mark.asyncio
    @patch('httpx.AsyncClient')
    async def test_upload_data_empty_list(self, mock_client_class, client):
        """Test upload_data with empty list."""
        # Mock the response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"status": "success"}'
        
        # Mock the async client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client
        
        # Call the method with empty list
        result = await client.upload_data([])
        
        # Check the JSON data structure
        call_args = mock_client.post.call_args
        json_data = call_args[1]["json"]
        assert json_data["format_version"] == 0
        assert json_data["api_key"] == "test_api_key"
        assert json_data["sets"] == []
    
    @pytest.mark.asyncio
    @patch('httpx.AsyncClient')
    async def test_upload_data_field_transformations(self, mock_client_class, client, sample_gearset):
        """Test that field transformations are applied correctly in upload_data."""
        # Mock the response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"status": "success"}'
        
        # Mock the async client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client
        
        # Call the method
        await client.upload_data([sample_gearset])
        
        # Get the JSON data from the call
        call_args = mock_client.post.call_args
        json_data = call_args[1]["json"]
        
        # Verify jsonable_encoder was used (indirectly by checking the structure)
        set_data = json_data["sets"][0]
        
        # Check the transformations were applied
        assert "set_id" in set_data
        assert "id" not in set_data
        assert set_data["set_id"] == sample_gearset.id
        
        trap_data = set_data["traps"][0]
        assert "trap_id" in trap_data  
        assert "id" not in trap_data
        assert trap_data["trap_id"] == sample_gearset.traps[0].id

    @pytest.mark.asyncio
    @patch('httpx.AsyncClient')
    async def test_upload_data_payload_structure(self, mock_client_class, client, sample_gearset):
        """Test the complete payload structure sent to the API."""
        # Mock the response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"status": "success"}'
        
        # Mock the async client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client
        
        # Call the method
        await client.upload_data([sample_gearset])
        
        # Get the JSON data from the call
        call_args = mock_client.post.call_args
        json_data = call_args[1]["json"]
        
        # Verify the top-level structure
        assert "sets" in json_data
        assert isinstance(json_data["sets"], list)
        assert len(json_data["sets"]) == 1
        
        # Verify set structure
        set_data = json_data["sets"][0]
        required_fields = ["vessel_id", "set_id", "deployment_type", "traps_in_set",
                          "trawl_path", "share_with", "traps", "when_updated_utc"]
        for field in required_fields:
            assert field in set_data
        
        # Verify trap structure
        assert len(set_data["traps"]) == 1
        trap_data = set_data["traps"][0]
        trap_fields = ["trap_id", "sequence", "latitude", "longitude", 
                      "deploy_datetime_utc", "surface_datetime_utc", 
                      "retrieved_datetime_utc", "status", "accuracy", 
                      "release_type", "is_on_end"]
        for field in trap_fields:
            assert field in trap_data
    
    def test_headers_constant(self):
        """Test that HEADERS constant is properly defined."""
        expected_headers = {
            "accept": "application/json",
            "Content-Type": "application/json"
        }
        assert RmwHubClient.HEADERS == expected_headers
    
    @pytest.mark.asyncio
    async def test_search_hub_datetime_timezone_conversion(self, client):
        """Test that datetime is properly converted to UTC."""
        # Create a datetime with a specific timezone
        eastern_tz = pytz.timezone('US/Eastern')
        local_datetime = datetime(2023, 9, 15, 10, 30, 0, tzinfo=eastern_tz)
        
        with patch('httpx.AsyncClient') as mock_client_class:
            # Mock the response
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = '{"sets": []}'
            
            # Mock the async client
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client
            
            # Call the method
            await client.search_hub(start_datetime=local_datetime)
            
            # Get the call arguments
            call_args = mock_client.post.call_args
            json_data = call_args[1]["json"]
            
            # Verify the datetime was converted to UTC
            expected_utc_iso = local_datetime.astimezone(pytz.utc).isoformat()
            assert json_data["start_datetime_utc"] == expected_utc_iso