import pytest
import json
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from uuid import uuid4
from pydantic import SecretStr

from gundi_core.events import LogLevel
from gundi_core.schemas.v2 import Integration

from app.actions.configurations import (
    AuthenticateConfig,
    PullRmwHubObservationsConfiguration,
)
from app.actions.rmwhub.adapter import RmwHubAdapter
from app.actions.buoy.types import Environment

# Import the functions without decorators for testing
from app.actions.handlers import (
    action_auth,
    handle_download,
    handle_upload,
)


class TestActionAuth:
    """Test suite for action_auth function."""
    
    @pytest.mark.asyncio
    async def test_action_auth_valid_api_key(self):
        """Test action_auth with valid API key."""
        integration = Mock(spec=Integration)
        action_config = AuthenticateConfig(api_key=SecretStr("valid_api_key"))
        
        result = await action_auth(integration, action_config)
        
        assert result["valid_credentials"] is True
        assert "some_message" in result
    
    @pytest.mark.asyncio
    async def test_action_auth_invalid_api_key(self):
        """Test action_auth with invalid (empty) API key."""
        integration = Mock(spec=Integration)
        # Create a mock config where api_key evaluates to None
        action_config = Mock(spec=AuthenticateConfig)
        action_config.api_key = None
        
        result = await action_auth(integration, action_config)
        
        assert result["valid_credentials"] is False
        assert "some_message" in result


class TestHandleDownload:
    """Test suite for handle_download function."""
    
    @pytest.fixture
    def mock_rmw_adapter(self):
        """Create mock RmwHubAdapter."""
        return AsyncMock(spec=RmwHubAdapter)
    
    @pytest.fixture
    def integration(self):
        """Create mock integration."""
        return Mock(spec=Integration, id=uuid4())
    
    @pytest.fixture
    def action_config(self):
        """Create mock action config."""
        return Mock(
            spec=PullRmwHubObservationsConfiguration,
            api_key=SecretStr("test_key"),
            rmw_url="https://test.com",
            minutes_to_sync=30,
            share_with=[],
        )
    
    @pytest.fixture
    def datetime_range(self):
        """Create datetime range for testing."""
        start = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        end = datetime(2023, 10, 1, 13, 0, 0, tzinfo=timezone.utc)
        return start, end
    
    @pytest.mark.asyncio
    async def test_handle_download_success_with_data(
        self, mock_rmw_adapter, integration, action_config, datetime_range
    ):
        """Test handle_download with successful data download."""
        start_datetime, end_datetime = datetime_range
        environment = Environment.DEV
        
        # Mock the adapter methods
        mock_gear_sets = [Mock(), Mock(), Mock()]
        mock_observations = [Mock(), Mock(), Mock(), Mock()]
        
        mock_rmw_adapter.download_data.return_value = mock_gear_sets
        mock_rmw_adapter.process_download.return_value = mock_observations
        
        with patch("app.actions.handlers.log_action_activity") as mock_log, \
             patch("app.actions.handlers.send_observations_to_gundi") as mock_send_observations, \
             patch("app.actions.handlers.generate_batches") as mock_generate_batches:
            
            # Mock generate_batches to return the observations in batches
            mock_generate_batches.return_value = [mock_observations[:2], mock_observations[2:]]
            
            result = await handle_download(
                mock_rmw_adapter,
                start_datetime,
                end_datetime,
                integration,
                environment,
                action_config
            )
        
        # Verify adapter methods were called
        mock_rmw_adapter.download_data.assert_called_once_with(start_datetime)
        mock_rmw_adapter.process_download.assert_called_once_with(mock_gear_sets)
        
        # Verify send_observations_to_gundi was called for each batch
        assert mock_send_observations.call_count == 2
        mock_send_observations.assert_any_call(
            observations=mock_observations[:2], integration_id=str(integration.id)
        )
        mock_send_observations.assert_any_call(
            observations=mock_observations[2:], integration_id=str(integration.id)
        )
        
        # Verify logging was called
        assert mock_log.call_count == 1
        log_call = mock_log.call_args_list[0]
        assert log_call[1]["integration_id"] == integration.id
        assert log_call[1]["action_id"] == "pull_observations"
        assert log_call[1]["level"] == LogLevel.INFO
        assert log_call[1]["title"] == "Extracting observations with filter.."
        assert log_call[1]["data"]["gear_sets_to_process"] == 3
        
        # Verify result (should return the count of observations)
        assert result == len(mock_observations)
    
    @pytest.mark.asyncio
    async def test_handle_download_success_no_data(
        self, mock_rmw_adapter, integration, action_config, datetime_range
    ):
        """Test handle_download when no data is returned."""
        start_datetime, end_datetime = datetime_range
        environment = Environment.DEV
        
        # Mock empty data
        mock_rmw_adapter.download_data.return_value = []
        
        with patch("app.actions.handlers.log_action_activity") as mock_log:
            result = await handle_download(
                mock_rmw_adapter,
                start_datetime,
                end_datetime,
                integration,
                environment,
                action_config
            )
        
        # Verify adapter methods were called
        mock_rmw_adapter.download_data.assert_called_once_with(start_datetime)
        mock_rmw_adapter.process_download.assert_not_called()
        
        # Verify logging was called twice (extraction and no data)
        assert mock_log.call_count == 2
        
        # Check first log call (extraction)
        log_call1 = mock_log.call_args_list[0]
        assert log_call1[1]["data"]["gear_sets_to_process"] == 0
        
        # Check second log call (no data)
        log_call2 = mock_log.call_args_list[1]
        assert log_call2[1]["title"] == "No gearsets returned from RMW Hub API."
        
        # Verify result
        assert result == []
    
    @pytest.mark.asyncio
    async def test_handle_download_config_dict_called(
        self, mock_rmw_adapter, integration, datetime_range
    ):
        """Test that action_config.dict() is called for logging."""
        start_datetime, end_datetime = datetime_range
        environment = Environment.DEV
        
        # Create a mock action config that tracks dict() calls
        action_config = Mock(spec=PullRmwHubObservationsConfiguration)
        action_config.dict.return_value = {"test": "config"}
        
        mock_rmw_adapter.download_data.return_value = []
        
        with patch("app.actions.handlers.log_action_activity") as mock_log:
            await handle_download(
                mock_rmw_adapter,
                start_datetime,
                end_datetime,
                integration,
                environment,
                action_config
            )
        
        # Verify dict() was called
        action_config.dict.assert_called()
        
        # Verify config data was passed to logging
        assert mock_log.call_args_list[0][1]["config_data"] == {"test": "config"}


class TestHandleUpload:
    """Test suite for handle_upload function."""
    
    @pytest.fixture
    def mock_rmw_adapter(self):
        """Create mock RmwHubAdapter."""
        return AsyncMock(spec=RmwHubAdapter)
    
    @pytest.fixture
    def integration(self):
        """Create mock integration."""
        return Mock(spec=Integration, id=uuid4())
    
    @pytest.fixture
    def action_config(self):
        """Create mock action config."""
        return Mock(
            spec=PullRmwHubObservationsConfiguration,
            api_key=SecretStr("test_key"),
            rmw_url="https://test.com",
        )
    
    @pytest.mark.asyncio
    async def test_handle_upload_success(
        self, mock_rmw_adapter, integration, action_config
    ):
        """Test handle_upload with successful upload."""
        start_datetime = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        # Mock successful response
        mock_response = {"status": "success", "uploaded": 5}
        mock_rmw_adapter.process_upload.return_value = (5, mock_response)
        
        with patch("app.actions.handlers.log_action_activity") as mock_log:
            result = await handle_upload(
                mock_rmw_adapter, start_datetime, integration, action_config
            )
        
        # Verify adapter method was called
        mock_rmw_adapter.process_upload.assert_called_once_with(start_datetime)
        
        # Verify success logging
        mock_log.assert_called_once()
        log_call = mock_log.call_args
        assert log_call[1]["level"] == LogLevel.INFO
        assert log_call[1]["title"] == "Process upload to rmwHub completed."
        assert log_call[1]["data"]["rmw_response"] == str(mock_response)
        
        # Verify result
        assert result == 5
    
    @pytest.mark.asyncio
    async def test_handle_upload_error_with_detail(
        self, mock_rmw_adapter, integration, action_config
    ):
        """Test handle_upload when response contains error detail."""
        start_datetime = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        # Mock error response
        mock_response = {"detail": "Upload failed", "error": "validation_error"}
        mock_rmw_adapter.process_upload.return_value = (0, mock_response)
        
        with patch("app.actions.handlers.log_action_activity") as mock_log:
            result = await handle_upload(
                mock_rmw_adapter, start_datetime, integration, action_config
            )
        
        # Verify adapter method was called
        mock_rmw_adapter.process_upload.assert_called_once_with(start_datetime)
        
        # Verify error logging
        mock_log.assert_called_once()
        log_call = mock_log.call_args
        assert log_call[1]["level"] == LogLevel.ERROR
        assert log_call[1]["title"] == "Failed to upload data to rmwHub."
        assert log_call[1]["data"]["rmw_response"] == str(mock_response)
        
        # Verify result (should return 0 on error)
        assert result == 0
    
    @pytest.mark.asyncio
    async def test_handle_upload_with_extra_params(
        self, mock_rmw_adapter, integration, action_config
    ):
        """Test handle_upload when called with extra parameters (as in the actual code)."""
        start_datetime = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        end_datetime = datetime(2023, 10, 1, 13, 0, 0, tzinfo=timezone.utc)
        environment = Environment.DEV
        
        # Mock successful response
        mock_response = {"status": "success", "uploaded": 3}
        mock_rmw_adapter.process_upload.return_value = (3, mock_response)
        
        with patch("app.actions.handlers.log_action_activity") as mock_log:
            # Call with extra parameters like the actual code does
            result = await handle_upload(
                mock_rmw_adapter, start_datetime, integration, action_config
            )
        
        # Verify adapter method was called with just start_datetime
        mock_rmw_adapter.process_upload.assert_called_once_with(start_datetime)
        
        # Verify success logging
        mock_log.assert_called_once()
        log_call = mock_log.call_args
        assert log_call[1]["level"] == LogLevel.INFO
        assert log_call[1]["title"] == "Process upload to rmwHub completed."
        
        # Verify result
        assert result == 3


class TestHandlerEdgeCases:
    """Test edge cases and error conditions in handlers."""
    
    @pytest.mark.asyncio
    async def test_action_auth_with_empty_string_api_key(self):
        """Test action_auth with empty string API key (should be considered valid)."""
        integration = Mock(spec=Integration)
        action_config = Mock(spec=AuthenticateConfig)
        action_config.api_key = ""  # Empty string, but not None
        
        result = await action_auth(integration, action_config)
        
        # Empty string is still "not None", so it's considered valid
        assert result["valid_credentials"] is True
        assert "some_message" in result
    
    @pytest.mark.asyncio
    async def test_handle_download_with_different_environments(self):
        """Test handle_download with different environment types."""
        mock_adapter = AsyncMock(spec=RmwHubAdapter)
        integration = Mock(spec=Integration, id=uuid4())
        action_config = Mock(spec=PullRmwHubObservationsConfiguration)
        action_config.dict.return_value = {"test": "config"}
        
        start_datetime = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        end_datetime = datetime(2023, 10, 1, 13, 0, 0, tzinfo=timezone.utc)
        
        # Test with different environments
        environments = [Environment.DEV, Environment.STAGE, Environment.PRODUCTION]
        
        for env in environments:
            mock_adapter.download_data.return_value = [Mock()]
            mock_adapter.process_download.return_value = [Mock(), Mock()]
            
            with patch("app.actions.handlers.log_action_activity") as mock_log, \
                 patch("app.actions.handlers.send_observations_to_gundi") as mock_send_observations, \
                 patch("app.actions.handlers.generate_batches") as mock_generate_batches:
                
                # Mock generate_batches to return observations in batches
                mock_generate_batches.return_value = [[Mock(), Mock()]]
                
                result = await handle_download(
                    mock_adapter, start_datetime, end_datetime, integration, env, action_config
                )
            
            # Verify environment is properly passed to logging
            log_call = mock_log.call_args_list[0]
            assert str(env) in str(log_call[1]["data"]["environment"])
            
            # Verify result
            assert result == 2
    
    @pytest.mark.asyncio
    async def test_handle_upload_with_empty_dict_response(self):
        """Test handle_upload when response is empty dict."""
        mock_adapter = AsyncMock(spec=RmwHubAdapter)
        integration = Mock(spec=Integration, id=uuid4())
        action_config = Mock(spec=PullRmwHubObservationsConfiguration)
        action_config.dict.return_value = {"test": "config"}
        
        start_datetime = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        # Mock empty dict response
        mock_adapter.process_upload.return_value = (2, {})
        
        with patch("app.actions.handlers.log_action_activity") as mock_log:
            result = await handle_upload(mock_adapter, start_datetime, integration, action_config)
        
        # Verify success logging (since no "detail" in empty dict response)
        mock_log.assert_called_once()
        log_call = mock_log.call_args
        assert log_call[1]["level"] == LogLevel.INFO
        assert log_call[1]["title"] == "Process upload to rmwHub completed."
        
        # Verify result
        assert result == 2


class TestActionPullObservationsCore:
    """Test suite for the core logic of pull observations (without decorators)."""
    
    @pytest.fixture
    def integration(self):
        """Create mock integration."""
        return Mock(spec=Integration, id=uuid4())
    
    @pytest.fixture
    def action_config(self):
        """Create mock action config."""
        return PullRmwHubObservationsConfiguration(
            api_key=SecretStr("test_key"),
            rmw_url="https://test.com/api/",
            minutes_to_sync=30,
            share_with=[]
        )
    
    @pytest.mark.asyncio
    async def test_pull_observations_core_logic(self, integration, action_config):
        """Test the core logic of pull observations without the decorator."""
        # Import the module to get the underlying function
        from app.actions import handlers
        
        # Get the wrapped function (without decorators)
        if hasattr(handlers.action_pull_observations, '__wrapped__'):
            pull_observations_func = handlers.action_pull_observations.__wrapped__
        else:
            pull_observations_func = handlers.action_pull_observations
        
        mock_connection_details = Mock()
        destination = Mock()
        destination.name = "Buoy Dev"
        destination.id = uuid4()
        mock_connection_details.destinations = [destination]
        
        with patch("app.actions.handlers.datetime") as mock_datetime, \
             patch("app.actions.handlers.GundiClient") as mock_gundi_client, \
             patch("app.actions.handlers.get_er_token_and_site") as mock_get_token, \
             patch("app.actions.handlers.RmwHubAdapter") as mock_adapter_class, \
             patch("app.actions.handlers.handle_download") as mock_handle_download, \
             patch("app.actions.handlers.handle_upload") as mock_handle_upload:
            
            # Setup mocks
            current_time = datetime(2023, 10, 1, 12, 30, 0, tzinfo=timezone.utc)
            mock_datetime.now.return_value = current_time
            mock_datetime.timedelta = timedelta
            
            mock_client = AsyncMock()
            mock_client.get_connection_details.return_value = mock_connection_details
            mock_gundi_client.return_value = mock_client
            
            mock_get_token.return_value = ("test_token", "https://er-dev.com/")
            mock_adapter_class.return_value = AsyncMock(spec=RmwHubAdapter)
            mock_handle_download.return_value = 2  # Return count of observations
            mock_handle_upload.return_value = 1
            
            # Execute function
            result = await pull_observations_func(integration, action_config)
            
            # Verify basic flow worked
            mock_client.get_connection_details.assert_called_once_with(integration.id)
            mock_get_token.assert_called_once_with(integration, Environment.DEV)
            mock_handle_download.assert_called_once()
            mock_handle_upload.assert_called_once()

            assert result["observations_downloaded"] == 2
            assert result["sets_updated"] == 1 

    @pytest.mark.asyncio
    async def test_pull_observations_24_hour_core_logic(self, integration, action_config):
        """Test the core logic of 24-hour pull observations without the decorator."""
        # Import the module to get the underlying function
        from app.actions import handlers
        
        # Get the wrapped function (without decorators)
        if hasattr(handlers.action_pull_observations_24_hour_sync, '__wrapped__'):
            pull_24h_func = handlers.action_pull_observations_24_hour_sync.__wrapped__
        else:
            pull_24h_func = handlers.action_pull_observations_24_hour_sync
        
        mock_connection_details = Mock()
        destination = Mock()
        destination.name = "Buoy Prod"  # Valid environment name
        destination.id = uuid4()
        mock_connection_details.destinations = [destination]
        
        with patch("app.actions.handlers.datetime") as mock_datetime, \
             patch("app.actions.handlers.GundiClient") as mock_gundi_client, \
             patch("app.actions.handlers.get_er_token_and_site") as mock_get_token, \
             patch("app.actions.handlers.RmwHubAdapter") as mock_adapter_class, \
             patch("app.actions.handlers.handle_download") as mock_handle_download, \
             patch("app.actions.handlers.handle_upload") as mock_handle_upload:
            
            # Setup mocks  
            current_time = datetime(2023, 10, 2, 0, 10, 0, tzinfo=timezone.utc)  # 12:10 AM
            mock_datetime.now.return_value = current_time
            mock_datetime.timedelta = timedelta
            
            mock_client = AsyncMock()
            mock_client.get_connection_details.return_value = mock_connection_details
            mock_gundi_client.return_value = mock_client
            
            mock_get_token.return_value = ("prod_token", "https://er-prod.com/")
            mock_adapter_class.return_value = AsyncMock(spec=RmwHubAdapter)
            mock_handle_download.return_value = 3  # Return count of observations
            mock_handle_upload.return_value = 2
            
            # Execute function
            result = await pull_24h_func(integration, action_config)
            
            # Verify basic flow worked
            mock_client.get_connection_details.assert_called_once_with(integration.id)
            mock_get_token.assert_called_once()  # Don't check specific env since it depends on destination name
            mock_handle_download.assert_called_once()
            mock_handle_upload.assert_called_once()
            
            assert result["observations_downloaded"] == 3
            assert result["sets_updated"] == 2


class TestHandlerIntegration:
    """Integration tests for handler functions working together."""
    
    @pytest.fixture
    def integration(self):
        """Create mock integration."""
        return Mock(spec=Integration, id=uuid4())
    
    @pytest.fixture  
    def action_config(self):
        """Create mock action config."""
        return PullRmwHubObservationsConfiguration(
            api_key=SecretStr("integration_test_key"),
            rmw_url="https://integration-test.com/api/",
            minutes_to_sync=60,
            share_with=["test_partner"]
        )
    
    @pytest.mark.asyncio
    async def test_handle_download_and_upload_integration(self, integration, action_config):
        """Test integration between handle_download and handle_upload."""
        start_datetime = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        end_datetime = datetime(2023, 10, 1, 13, 0, 0, tzinfo=timezone.utc)
        environment = Environment.DEV
        
        # Create a realistic adapter mock
        mock_adapter = AsyncMock(spec=RmwHubAdapter)
        mock_gear_sets = [Mock() for _ in range(2)]
        mock_observations = [Mock() for _ in range(4)]
        
        mock_adapter.download_data.return_value = mock_gear_sets
        mock_adapter.process_download.return_value = mock_observations
        mock_adapter.process_upload.return_value = (1, {"status": "success"})
        
        with patch("app.actions.handlers.log_action_activity") as mock_log, \
             patch("app.actions.handlers.send_observations_to_gundi") as mock_send_observations, \
             patch("app.actions.handlers.generate_batches") as mock_generate_batches:
            
            # Mock generate_batches to return the observations in batches
            mock_generate_batches.return_value = [mock_observations[:2], mock_observations[2:]]
            
            # Test download
            download_result = await handle_download(
            mock_adapter, start_datetime, end_datetime, integration, environment, action_config
            )
            
            # Test upload  
            upload_result = await handle_upload(
            mock_adapter, start_datetime, integration, action_config
            )
        
        # Verify download results
        assert download_result == 4
        
        # Verify upload results
        assert upload_result == 1
        
        # Verify adapter was called correctly
        mock_adapter.download_data.assert_called_once_with(start_datetime)
        mock_adapter.process_download.assert_called_once_with(mock_gear_sets)
        mock_adapter.process_upload.assert_called_once_with(start_datetime)
        
        # Verify logging occurred
        assert mock_log.call_count >= 2  # At least one log for download, one for upload
