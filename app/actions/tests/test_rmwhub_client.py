import pytest
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx
from fastapi.encoders import jsonable_encoder

from app.actions.rmwhub.client import RmwHubClient, SEARCH_PAGE_SIZE, MAX_SEARCH_PAGES
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
        return datetime(2023, 9, 15, 14, 30, 0, tzinfo=timezone(timedelta(hours=-4)))
    
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
            trawl_path={"path": "path_001"},
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
        assert isinstance(client.default_timeout, httpx.Timeout)
        assert RmwHubClient.HEADERS == {
            "accept": "application/json",
            "Content-Type": "application/json"
        }

    def test_init_with_custom_timeouts(self):
        """Test RmwHubClient initialization with custom timeouts."""
        api_key = "test_key_123"
        rmw_url = "https://example.rmwhub.com"

        client = RmwHubClient(
            api_key=api_key,
            rmw_url=rmw_url,
            default_timeout=90.0,
            connect_timeout=15.0,
            read_timeout=90.0
        )

        assert client.api_key == api_key
        assert client.rmw_url == rmw_url
        assert isinstance(client.default_timeout, httpx.Timeout)
    
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
            "max_sets": 1000,
            "start_datetime_utc": sample_datetime.astimezone(timezone.utc).isoformat(),
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
            "RMW Hub API error | POST /search_hub/ | HTTP %s: %s",
            400,
            '{"error": "Bad request"}',
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
            trawl_path={"path": "path_001"},
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
            trawl_path={"path": "path_002"},
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
            trawl_path={},
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
        mock_response.text = '{"error": "Internal server error"}'
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

        # Verify error was logged with structured format
        mock_logger.error.assert_called_once_with(
            "RMW Hub API error | POST /upload_deployments/ | HTTP %s: %s (set_ids=%s)",
            500,
            '{"error": "Internal server error"}',
            ["gearset_001"],
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
        eastern_tz = timezone(timedelta(hours=-4))
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
            expected_utc_iso = local_datetime.astimezone(timezone.utc).isoformat()
            assert json_data["start_datetime_utc"] == expected_utc_iso

    @pytest.mark.asyncio
    @patch('httpx.AsyncClient')
    async def test_upload_data_timeout(self, mock_client_class, client, sample_gearset):
        """Test upload_data when request times out."""
        # Mock the async client to raise a timeout exception
        mock_client = AsyncMock()
        mock_client.post.side_effect = httpx.ReadTimeout("Request timed out after 60 seconds")
        mock_client_class.return_value.__aenter__.return_value = mock_client

        # Call the method and expect the timeout exception to propagate
        with pytest.raises(httpx.ReadTimeout) as exc_info:
            await client.upload_data([sample_gearset])

        # Verify the exception message
        assert "Request timed out" in str(exc_info.value)

    @pytest.mark.asyncio
    @patch('app.actions.rmwhub.client.asyncio.sleep', new_callable=AsyncMock)
    @patch('httpx.AsyncClient')
    async def test_search_hub_timeout(self, mock_client_class, mock_sleep, client, sample_datetime):
        """Test search_hub raises after exhausting retries on timeout."""
        # Mock the async client to raise a timeout exception
        mock_client = AsyncMock()
        mock_client.post.side_effect = httpx.ReadTimeout("Request timed out after 60 seconds")
        mock_client_class.return_value.__aenter__.return_value = mock_client

        with pytest.raises(httpx.ReadTimeout):
            await client.search_hub(start_datetime=sample_datetime)

        assert mock_client.post.call_count == 3  # RETRY_COUNT
        assert mock_sleep.call_count == 2  # RETRY_COUNT - 1

    @pytest.mark.asyncio
    @patch('httpx.AsyncClient')
    async def test_upload_data_connect_timeout(self, mock_client_class, client, sample_gearset):
        """Test upload_data when connection times out."""
        # Mock the async client to raise a connect timeout exception
        mock_client = AsyncMock()
        mock_client.post.side_effect = httpx.ConnectTimeout("Connection timed out after 10 seconds")
        mock_client_class.return_value.__aenter__.return_value = mock_client

        # Call the method and expect the timeout exception to propagate
        with pytest.raises(httpx.ConnectTimeout) as exc_info:
            await client.upload_data([sample_gearset])

        # Verify the exception message
        assert "Connection timed out" in str(exc_info.value)

    @pytest.mark.asyncio
    @patch('app.actions.rmwhub.client.asyncio.sleep', new_callable=AsyncMock)
    @patch('httpx.AsyncClient')
    async def test_upload_data_retry_then_success(self, mock_client_class, mock_sleep, client, sample_gearset):
        """Test upload_data retries on 502 and succeeds on second attempt."""
        mock_502 = MagicMock()
        mock_502.status_code = 502
        mock_502.content = b'Bad Gateway'

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.content = b'{"status": "success"}'

        mock_client = AsyncMock()
        mock_client.post.side_effect = [mock_502, mock_200]
        mock_client_class.return_value.__aenter__.return_value = mock_client

        result = await client.upload_data([sample_gearset])

        assert result.status_code == 200
        assert mock_client.post.call_count == 2
        mock_sleep.assert_called_once_with(5)

    @pytest.mark.asyncio
    @patch('app.actions.rmwhub.client.asyncio.sleep', new_callable=AsyncMock)
    @patch('httpx.AsyncClient')
    @patch('app.actions.rmwhub.client.logger')
    async def test_upload_data_retry_exhausted(self, mock_logger, mock_client_class, mock_sleep, client, sample_gearset):
        """Test upload_data fails after exhausting all retries on 503."""
        mock_503 = MagicMock()
        mock_503.status_code = 503
        mock_503.content = b'Service Unavailable'

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_503
        mock_client_class.return_value.__aenter__.return_value = mock_client

        result = await client.upload_data([sample_gearset])

        assert result.status_code == 503
        assert mock_client.post.call_count == 3  # RETRY_COUNT
        assert mock_sleep.call_count == 2  # retries - 1
        mock_logger.error.assert_called()

    @pytest.mark.asyncio
    @patch('httpx.AsyncClient')
    async def test_upload_data_non_retryable_error(self, mock_client_class, client, sample_gearset):
        """Test upload_data does not retry on non-retryable status codes like 400."""
        mock_400 = MagicMock()
        mock_400.status_code = 400
        mock_400.content = b'Bad Request'

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_400
        mock_client_class.return_value.__aenter__.return_value = mock_client

        result = await client.upload_data([sample_gearset])

        assert result.status_code == 400
        assert mock_client.post.call_count == 1  # No retries


def _make_sets(set_ids, when_updated_utc):
    """Helper to build a list of minimal set dicts for search_hub_all tests."""
    return [{"set_id": sid, "when_updated_utc": when_updated_utc} for sid in set_ids]


def _search_response(sets):
    """Helper to build a JSON response string from a list of set dicts."""
    return json.dumps({"format_version": 0.1, "sets": sets})


class TestSearchHubAll:
    """Tests for RmwHubClient.search_hub_all pagination logic."""

    @pytest.fixture
    def client(self):
        return RmwHubClient(api_key="test_api_key", rmw_url="https://test.rmwhub.com")

    @pytest.fixture
    def start_dt(self):
        return datetime(2024, 1, 1, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_single_page_under_page_size(self, client, start_dt):
        """A response with fewer sets than SEARCH_PAGE_SIZE ends pagination."""
        sets = _make_sets(["s1", "s2"], "2024-01-02T00:00:00Z")
        with patch.object(client, "search_hub", new_callable=AsyncMock) as mock_search:
            mock_search.return_value = _search_response(sets)
            result = await client.search_hub_all(start_dt)

        assert len(result["sets"]) == 2
        assert {s["set_id"] for s in result["sets"]} == {"s1", "s2"}
        mock_search.assert_called_once_with(start_dt)

    @pytest.mark.asyncio
    async def test_multi_page_pagination(self, client, start_dt):
        """Full pages advance the cursor; a short final page terminates."""
        page1_sets = _make_sets(
            [f"p1_{i}" for i in range(SEARCH_PAGE_SIZE)],
            "2024-01-05T00:00:00Z",
        )
        page2_sets = _make_sets(["p2_0", "p2_1"], "2024-01-06T00:00:00Z")

        with patch.object(client, "search_hub", new_callable=AsyncMock) as mock_search:
            mock_search.side_effect = [
                _search_response(page1_sets),
                _search_response(page2_sets),
            ]
            result = await client.search_hub_all(start_dt)

        assert len(result["sets"]) == SEARCH_PAGE_SIZE + 2
        assert mock_search.call_count == 2
        # Second call should use the advanced cursor
        second_call_dt = mock_search.call_args_list[1][0][0]
        assert second_call_dt == datetime(2024, 1, 5, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    async def test_deduplication_across_pages(self, client, start_dt):
        """Sets appearing on multiple pages are deduplicated by set_id."""
        shared_id = "shared"
        page1_sets = _make_sets(
            [shared_id] + [f"p1_{i}" for i in range(SEARCH_PAGE_SIZE - 1)],
            "2024-01-05T00:00:00Z",
        )
        page2_sets = _make_sets([shared_id, "p2_new"], "2024-01-06T00:00:00Z")

        with patch.object(client, "search_hub", new_callable=AsyncMock) as mock_search:
            mock_search.side_effect = [
                _search_response(page1_sets),
                _search_response(page2_sets),
            ]
            result = await client.search_hub_all(start_dt)

        set_ids = [s["set_id"] for s in result["sets"]]
        assert set_ids.count(shared_id) == 1
        assert len(result["sets"]) == SEARCH_PAGE_SIZE + 1  # page1 + p2_new

    @pytest.mark.asyncio
    async def test_stall_detection_no_new_sets(self, client, start_dt):
        """Stops when a full page contains only already-seen set_ids."""
        same_sets = _make_sets(
            [f"s{i}" for i in range(SEARCH_PAGE_SIZE)],
            "2024-01-05T00:00:00Z",
        )
        with patch.object(client, "search_hub", new_callable=AsyncMock) as mock_search:
            # Return identical pages — second page has 0 new sets
            mock_search.side_effect = [
                _search_response(same_sets),
                _search_response(same_sets),
            ]
            result = await client.search_hub_all(start_dt)

        assert len(result["sets"]) == SEARCH_PAGE_SIZE
        # Should stop after 2 calls (page 2 has new_count == 0)
        assert mock_search.call_count == 2

    @pytest.mark.asyncio
    async def test_stall_detection_cursor_not_advancing(self, client, start_dt):
        """Stops when max(when_updated_utc) doesn't exceed current cursor."""
        # All timestamps equal the start — cursor can't advance
        page_sets = _make_sets(
            [f"s{i}" for i in range(SEARCH_PAGE_SIZE)],
            start_dt.isoformat(),
        )
        with patch.object(client, "search_hub", new_callable=AsyncMock) as mock_search:
            mock_search.return_value = _search_response(page_sets)
            result = await client.search_hub_all(start_dt)

        assert len(result["sets"]) == SEARCH_PAGE_SIZE
        mock_search.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_sets_stops(self, client, start_dt):
        """An empty sets array on the first page returns no data."""
        with patch.object(client, "search_hub", new_callable=AsyncMock) as mock_search:
            mock_search.return_value = _search_response([])
            result = await client.search_hub_all(start_dt)

        assert result["sets"] == []
        mock_search.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_json_stops(self, client, start_dt):
        """Non-JSON response stops pagination and returns what we have so far."""
        with patch.object(client, "search_hub", new_callable=AsyncMock) as mock_search:
            mock_search.return_value = "NOT VALID JSON"
            result = await client.search_hub_all(start_dt)

        assert result["sets"] == []

    @pytest.mark.asyncio
    async def test_mixed_timestamp_formats(self, client, start_dt):
        """Cursor correctly picks the latest time across different ISO formats."""
        sets = [
            {"set_id": "a", "when_updated_utc": "2024-01-10T12:00:00Z"},
            {"set_id": "b", "when_updated_utc": "2024-01-10T12:00:00+00:00"},
            {"set_id": "c", "when_updated_utc": "2024-01-11T00:00:00Z"},  # latest
        ]
        # Pad to full page so pagination tries to continue
        for i in range(SEARCH_PAGE_SIZE - len(sets)):
            sets.append({"set_id": f"pad_{i}", "when_updated_utc": "2024-01-09T00:00:00Z"})

        with patch.object(client, "search_hub", new_callable=AsyncMock) as mock_search:
            mock_search.side_effect = [
                _search_response(sets),
                _search_response([]),  # second page empty
            ]
            result = await client.search_hub_all(start_dt)

        assert mock_search.call_count == 2
        second_call_dt = mock_search.call_args_list[1][0][0]
        assert second_call_dt == datetime(2024, 1, 11, tzinfo=timezone.utc)

    @pytest.mark.asyncio
    @patch("app.actions.rmwhub.client.MAX_SEARCH_PAGES", 3)
    async def test_max_pages_exhausted_logs_warning(self, client, start_dt):
        """Logs a warning when MAX_SEARCH_PAGES is reached with full pages."""
        page_num = [0]

        async def _next_page(dt):
            page_num[0] += 1
            sets = _make_sets(
                [f"pg{page_num[0]}_{i}" for i in range(SEARCH_PAGE_SIZE)],
                f"2024-01-{page_num[0] + 1:02d}T00:00:00Z",
            )
            return _search_response(sets)

        with patch.object(client, "search_hub", side_effect=_next_page):
            with patch("app.actions.rmwhub.client.logger") as mock_logger:
                result = await client.search_hub_all(start_dt)

        assert len(result["sets"]) == SEARCH_PAGE_SIZE * 3
        mock_logger.warning.assert_any_call(
            "Reached MAX_SEARCH_PAGES (%d) — results may be incomplete. "
            "Fetched %d sets so far; consider increasing MAX_SEARCH_PAGES or "
            "narrowing the start_datetime window.",
            3,
            SEARCH_PAGE_SIZE * 3,
        )

    @pytest.mark.asyncio
    async def test_unparseable_timestamps_stops(self, client, start_dt):
        """If no when_updated_utc can be parsed, pagination stops safely."""
        sets = _make_sets(
            [f"s{i}" for i in range(SEARCH_PAGE_SIZE)],
            "not-a-timestamp",
        )
        with patch.object(client, "search_hub", new_callable=AsyncMock) as mock_search:
            mock_search.return_value = _search_response(sets)
            result = await client.search_hub_all(start_dt)

        assert len(result["sets"]) == SEARCH_PAGE_SIZE
        mock_search.assert_called_once()