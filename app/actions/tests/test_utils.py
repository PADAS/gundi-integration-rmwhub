import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.actions.utils import generate_batches, get_er_token_and_site, LOAD_BATCH_SIZE
from app.actions.buoy.types import Environment


class TestGenerateBatches:
    """Test cases for the generate_batches function."""
    
    def test_generate_batches_default_size(self):
        """Test generate_batches with default batch size."""
        data = list(range(250))  # 250 items
        batches = list(generate_batches(data))
        
        assert len(batches) == 3  # 100, 100, 50
        assert len(batches[0]) == LOAD_BATCH_SIZE
        assert len(batches[1]) == LOAD_BATCH_SIZE
        assert len(batches[2]) == 50
        assert batches[0] == list(range(100))
        assert batches[1] == list(range(100, 200))
        assert batches[2] == list(range(200, 250))
    
    def test_generate_batches_custom_size(self):
        """Test generate_batches with custom batch size."""
        data = list(range(25))
        batch_size = 10
        batches = list(generate_batches(data, batch_size))
        
        assert len(batches) == 3  # 10, 10, 5
        assert len(batches[0]) == 10
        assert len(batches[1]) == 10
        assert len(batches[2]) == 5
        assert batches[0] == list(range(10))
        assert batches[1] == list(range(10, 20))
        assert batches[2] == list(range(20, 25))
    
    def test_generate_batches_empty_list(self):
        """Test generate_batches with empty list."""
        data = []
        batches = list(generate_batches(data))
        
        assert len(batches) == 0
    
    def test_generate_batches_single_item(self):
        """Test generate_batches with single item."""
        data = [1]
        batches = list(generate_batches(data))
        
        assert len(batches) == 1
        assert batches[0] == [1]
    
    def test_generate_batches_exact_batch_size(self):
        """Test generate_batches when data length equals batch size."""
        data = list(range(LOAD_BATCH_SIZE))
        batches = list(generate_batches(data))
        
        assert len(batches) == 1
        assert len(batches[0]) == LOAD_BATCH_SIZE
        assert batches[0] == data
    
    def test_generate_batches_with_strings(self):
        """Test generate_batches with string data."""
        data = [f"item_{i}" for i in range(15)]
        batch_size = 5
        batches = list(generate_batches(data, batch_size))
        
        assert len(batches) == 3
        assert batches[0] == ["item_0", "item_1", "item_2", "item_3", "item_4"]
        assert batches[1] == ["item_5", "item_6", "item_7", "item_8", "item_9"]
        assert batches[2] == ["item_10", "item_11", "item_12", "item_13", "item_14"]


class TestGetErTokenAndSite:
    """Test cases for the get_er_token_and_site function."""
    
    @pytest.fixture
    def mock_integration(self):
        """Mock integration fixture."""
        # Create a mock Integration using MagicMock to avoid pydantic validation issues
        mock_integration = MagicMock()
        mock_integration.id = "test-integration-id"
        mock_integration.name = "Test Integration"
        mock_integration.configurations = []
        mock_integration.enabled = True
        return mock_integration
    
    @pytest.fixture
    def mock_connection_details(self):
        """Mock connection details fixture."""
        # Create a mock Connection using MagicMock to avoid pydantic validation issues
        mock_connection = MagicMock()
        mock_destination1 = MagicMock()
        mock_destination1.id = "dest-1"
        mock_destination1.name = "Production Buoy"
        
        mock_destination2 = MagicMock()
        mock_destination2.id = "dest-2"
        mock_destination2.name = "Buoy Dev Environment"
        
        mock_destination3 = MagicMock()
        mock_destination3.id = "dest-3"
        mock_destination3.name = "Staging Buoy"
        
        mock_connection.destinations = [mock_destination1, mock_destination2, mock_destination3]
        return mock_connection
    
    @pytest.fixture
    def mock_destination_details(self):
        """Mock destination details fixture."""
        mock_config = MagicMock()
        mock_config.action.value = "auth"
        mock_config.data = {
            "token": "test-token-123",
            "base_url": "https://test.earthranger.com"
        }
        
        return MagicMock(
            id="dest-2",
            base_url="https://test.earthranger.com",
            configurations=[mock_config]
        )
    
    @pytest.fixture
    def mock_auth_config(self):
        """Mock auth configuration fixture."""
        mock_auth = MagicMock()
        mock_auth.token = "test-token-123"
        return mock_auth
    
    @patch('app.actions.utils.GundiClient')
    @patch('app.actions.utils.find_config_for_action')
    @patch('app.actions.utils.schemas.v2.ERAuthActionConfig.parse_obj')
    @pytest.mark.asyncio
    async def test_get_er_token_and_site_success(
        self, 
        mock_parse_obj,
        mock_find_config,
        mock_gundi_client_class,
        mock_integration,
        mock_connection_details,
        mock_destination_details,
        mock_auth_config
    ):
        """Test successful retrieval of ER token and site."""
        # Setup mocks
        mock_client = AsyncMock()
        mock_gundi_client_class.return_value = mock_client
        mock_client.get_connection_details.return_value = mock_connection_details
        mock_client.get_integration_details.return_value = mock_destination_details
        
        mock_find_config.return_value = mock_destination_details.configurations[0]
        mock_parse_obj.return_value = mock_auth_config
        mock_auth_config.token = "test-token-123"
        
        # Test
        environment = Environment.DEV
        token, site = await get_er_token_and_site(mock_integration, environment)
        
        # Assertions
        assert token == "test-token-123"
        assert site == "https://test.earthranger.com"
        
        mock_client.get_connection_details.assert_called_once_with("test-integration-id")
        mock_client.get_integration_details.assert_called_once_with("dest-2")
        mock_find_config.assert_called_once_with(
            configurations=mock_destination_details.configurations,
            action_id="auth"
        )
        mock_parse_obj.assert_called_once()
    
    @patch('app.actions.utils.GundiClient')
    @patch('app.actions.utils.find_config_for_action')
    @patch('app.actions.utils.schemas.v2.ERAuthActionConfig.parse_obj')
    @pytest.mark.asyncio
    async def test_get_er_token_and_site_no_auth_config(
        self,
        mock_parse_obj,
        mock_find_config,
        mock_gundi_client_class,
        mock_integration,
        mock_connection_details,
        mock_destination_details
    ):
        """Test when no auth config is found."""
        # Setup mocks
        mock_client = AsyncMock()
        mock_gundi_client_class.return_value = mock_client
        mock_client.get_connection_details.return_value = mock_connection_details
        mock_client.get_integration_details.return_value = mock_destination_details
        
        mock_find_config.return_value = mock_destination_details.configurations[0]
        mock_parse_obj.return_value = None  # No auth config
        
        # Test
        environment = Environment.DEV
        token, site = await get_er_token_and_site(mock_integration, environment)
        
        # Assertions
        assert token is None
        assert site is None
    
    @patch('app.actions.utils.GundiClient')
    @pytest.mark.asyncio
    async def test_get_er_token_and_site_no_matching_destination(
        self,
        mock_gundi_client_class,
        mock_integration
    ):
        """Test when no destination matches the environment."""
        # Setup mocks
        mock_client = AsyncMock()
        mock_gundi_client_class.return_value = mock_client
        
        # Create connection details without matching environment
        mock_connection_details = MagicMock()
        mock_destination1 = MagicMock()
        mock_destination1.id = "dest-1"
        mock_destination1.name = "Production Environment"
        
        mock_destination2 = MagicMock()
        mock_destination2.id = "dest-2"
        mock_destination2.name = "Staging Environment"
        
        mock_connection_details.destinations = [mock_destination1, mock_destination2]
        mock_client.get_connection_details.return_value = mock_connection_details
        
        # Test - this should raise RuntimeError (converted from StopIteration in Python 3.7+) when no matching destination is found
        environment = Environment.DEV  # "Buoy Dev" won't match any destination names
        
        with pytest.raises(RuntimeError):
            await get_er_token_and_site(mock_integration, environment)
    
    @patch('app.actions.utils.GundiClient')
    @patch('app.actions.utils.find_config_for_action')
    @pytest.mark.asyncio
    async def test_get_er_token_and_site_no_config_found(
        self,
        mock_find_config,
        mock_gundi_client_class,
        mock_integration,
        mock_connection_details,
        mock_destination_details
    ):
        """Test when find_config_for_action returns None."""
        # Setup mocks
        mock_client = AsyncMock()
        mock_gundi_client_class.return_value = mock_client
        mock_client.get_connection_details.return_value = mock_connection_details
        mock_client.get_integration_details.return_value = mock_destination_details
        
        mock_find_config.return_value = None  # No config found
        
        # Test - this should raise AttributeError when trying to access .data on None
        environment = Environment.DEV
        
        with pytest.raises(AttributeError):
            await get_er_token_and_site(mock_integration, environment)
    
    @patch('app.actions.utils.GundiClient')
    @patch('app.actions.utils.find_config_for_action')
    @patch('app.actions.utils.schemas.v2.ERAuthActionConfig.parse_obj')
    @pytest.mark.asyncio
    async def test_get_er_token_and_site_different_environments(
        self,
        mock_parse_obj,
        mock_find_config,
        mock_gundi_client_class,
        mock_integration,
        mock_destination_details,
        mock_auth_config
    ):
        """Test with different environment values."""
        mock_client = AsyncMock()
        mock_gundi_client_class.return_value = mock_client
        
        # Test PRODUCTION environment
        mock_connection_details_prod = MagicMock()
        mock_dest_prod = MagicMock()
        mock_dest_prod.id = "dest-prod"
        mock_dest_prod.name = "Buoy Prod System"
        mock_connection_details_prod.destinations = [mock_dest_prod]
        
        mock_client.get_connection_details.return_value = mock_connection_details_prod
        mock_client.get_integration_details.return_value = mock_destination_details
        mock_find_config.return_value = mock_destination_details.configurations[0]
        mock_parse_obj.return_value = mock_auth_config
        mock_auth_config.token = "prod-token"
        
        environment = Environment.PRODUCTION
        token, site = await get_er_token_and_site(mock_integration, environment)
        
        assert token == "prod-token"
        assert site == "https://test.earthranger.com"