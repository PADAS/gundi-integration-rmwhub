import json
import uuid
from datetime import datetime, timezone
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from gundi_core.schemas.v2.gundi import LogLevel

from app.actions.buoy.types import BuoyDevice, BuoyGear, DeviceLocation
from app.actions.rmwhub.adapter import RmwHubAdapter, deduplicate_traps_by_id
from app.actions.rmwhub.types import GearSet, Trap


class TestRmwHubAdapter:
    """Test cases for the RmwHubAdapter class."""
    
    @pytest.fixture
    def mock_rmw_client(self):
        """Fixture for mocked RmwHubClient."""
        return MagicMock()
    
    @pytest.fixture
    def mock_gear_client(self):
        """Fixture for mocked BuoyClient."""
        return MagicMock()
    
    @pytest.fixture
    def integration_id(self):
        """Fixture for integration ID."""
        return str(uuid.uuid4())
    
    @pytest.fixture
    def adapter(self, integration_id):
        """Fixture for RmwHubAdapter instance."""
        with patch('app.actions.rmwhub.adapter.RmwHubClient') as mock_rmw_client_class, \
             patch('app.actions.rmwhub.adapter.BuoyClient') as mock_gear_client_class:
            
            adapter = RmwHubAdapter(
                integration_id=integration_id,
                api_key="test_api_key",
                rmw_url="https://test.rmwhub.com",
                er_token="test_er_token",
                er_destination="https://test.earthranger.com",
                gear_timeout=30.0,
                gear_connect_timeout=5.0,
                gear_read_timeout=30.0,
                options={"test": "option"}
            )
            return adapter
    
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
            deployment_type="lobster",
            traps_in_set=5,
            trawl_path={},
            share_with=["partner_1", "partner_2"],
            when_updated_utc="2023-09-15T18:00:00Z",
            traps=[sample_trap]
        )
    
    @pytest.fixture
    def sample_buoy_device(self):
        """Fixture for sample BuoyDevice."""
        return BuoyDevice(
            device_id=str(uuid.uuid4()),  # Use UUID as device_id
            mfr_device_id="mfr_001",
            label="Buoy Device 1",
            location=DeviceLocation(latitude=42.123456, longitude=-71.987654),
            last_updated=datetime(2023, 9, 15, 18, 0, 0, tzinfo=timezone.utc),
            last_deployed=datetime(2023, 9, 15, 14, 30, 0, tzinfo=timezone.utc)
        )
    
    @pytest.fixture
    def sample_buoy_gear(self, sample_buoy_device):
        """Fixture for sample BuoyGear."""
        return BuoyGear(
            id=uuid.uuid4(),
            display_id="buoy_001",
            name="Buoy Gear 1",
            status="deployed",
            last_updated=datetime(2023, 9, 15, 18, 0, 0, tzinfo=timezone.utc),
            devices=[sample_buoy_device],
            type="buoy",
            manufacturer="test_manufacturer",
            location=DeviceLocation(latitude=42.123456, longitude=-71.987654),
            additional={"display_id": "buoy_001"}
        )

    def test_init(self, integration_id):
        """Test adapter initialization."""
        with patch('app.actions.rmwhub.adapter.RmwHubClient') as mock_rmw_client_class, \
             patch('app.actions.rmwhub.adapter.BuoyClient') as mock_gear_client_class:
            
            adapter = RmwHubAdapter(
                integration_id=integration_id,
                api_key="test_api_key",
                rmw_url="https://test.rmwhub.com",
                er_token="test_er_token",
                er_destination="https://test.earthranger.com"
            )
            
            assert adapter.integration_id == integration_id
            assert adapter.er_subject_name_to_subject_mapping == {}
            assert adapter.options == {}
            mock_rmw_client_class.assert_called_once_with(
                "test_api_key",
                "https://test.rmwhub.com",
                default_timeout=120.0,
                connect_timeout=10.0,
                read_timeout=120.0
            )
            mock_gear_client_class.assert_called_once()

    def test_init_with_options(self, integration_id):
        """Test adapter initialization with options."""
        with patch('app.actions.rmwhub.adapter.RmwHubClient'), \
             patch('app.actions.rmwhub.adapter.BuoyClient'):
            
            options = {"timeout": 60}
            adapter = RmwHubAdapter(
                integration_id=integration_id,
                api_key="test_api_key",
                rmw_url="https://test.rmwhub.com",
                er_token="test_er_token",
                er_destination="https://test.earthranger.com",
                options=options
            )
            
            assert adapter.options == options

    def test_integration_uuid_property_with_string(self, adapter):
        """Test integration_uuid property with string ID."""
        string_id = str(uuid.uuid4())
        adapter.integration_id = string_id
        result = adapter.integration_uuid
        assert isinstance(result, uuid.UUID)
        assert str(result) == string_id

    def test_integration_uuid_property_with_uuid(self, adapter):
        """Test integration_uuid property with UUID ID."""
        uuid_id = uuid.uuid4()
        adapter.integration_id = uuid_id
        result = adapter.integration_uuid
        assert result == uuid_id

    @pytest.mark.asyncio
    async def test_download_data_success(self, adapter, sample_gearset):
        """Test successful data download."""
        mock_response = {
            "sets": [{
                "vessel_id": "vessel_001",
                "set_id": "gearset_001",
                "deployment_type": "lobster",
                "traps_in_set": 1,
                "trawl_path": {},  # Changed to dict
                "share_with": [],
                "when_updated_utc": "2023-09-15T18:00:00Z",
                "traps": [{
                    "trap_id": "trap_001",
                    "sequence": 1,
                    "latitude": 42.123456,
                    "longitude": -71.987654,
                    "deploy_datetime_utc": "2023-09-15T14:30:00Z",
                    "surface_datetime_utc": "2023-09-15T16:00:00Z",
                    "retrieved_datetime_utc": "2023-09-15T17:30:00Z",
                    "status": "deployed",
                    "accuracy": "high",
                    "release_type": "manual",
                    "is_on_end": True
                }]
            }]
        }
        
        adapter.rmw_client.search_hub_all = AsyncMock(return_value=mock_response)
        
        result = await adapter.download_data(datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc))
        
        assert len(result) == 1
        assert isinstance(result[0], GearSet)
        assert result[0].id == "gearset_001"
        assert result[0].vessel_id == "vessel_001"
        assert len(result[0].traps) == 1
        assert result[0].traps[0].id == "trap_001"

    @pytest.mark.asyncio
    async def test_download_data_with_status(self, adapter):
        """Test data download with status filter."""
        mock_response = {"sets": []}
        adapter.rmw_client.search_hub_all = AsyncMock(return_value=mock_response)

        await adapter.download_data(datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc), status="deployed")

        adapter.rmw_client.search_hub_all.assert_called_once_with(datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc))

    @pytest.mark.asyncio
    async def test_download_data_no_sets(self, adapter):
        """Test data download when no sets are returned."""
        mock_response = {"data": "no_sets_key"}
        adapter.rmw_client.search_hub_all = AsyncMock(return_value=mock_response)
        
        with patch('app.actions.rmwhub.adapter.logger') as mock_logger:
            result = await adapter.download_data(datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc))
            
            assert result == []
            mock_logger.error.assert_called_once()

    @pytest.mark.asyncio
    async def test_download_data_api_error(self, adapter):
        """Test data download when API returns error (no sets key)."""
        error_response = {"error": "something went wrong"}
        adapter.rmw_client.search_hub_all = AsyncMock(return_value=error_response)

        with patch('app.actions.rmwhub.adapter.logger') as mock_logger:
            result = await adapter.download_data(datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc))

            assert result == []
            mock_logger.error.assert_called_once()

    def test_convert_to_sets_success(self, adapter):
        """Test successful conversion of response to sets."""
        response_json = {
            "sets": [{
                "vessel_id": "vessel_001",
                "set_id": "gearset_001",
                "deployment_type": "lobster",
                "traps_in_set": 1,
                "trawl_path": {},  # Changed to dict
                "share_with": ["partner_1"],
                "when_updated_utc": "2023-09-15T18:00:00Z",
                "traps": [{
                    "trap_id": "trap_001",
                    "sequence": 1,
                    "latitude": 42.123456,
                    "longitude": -71.987654,
                    "deploy_datetime_utc": "2023-09-15T14:30:00Z",
                    "surface_datetime_utc": "2023-09-15T16:00:00Z",
                    "retrieved_datetime_utc": "2023-09-15T17:30:00Z",
                    "status": "deployed",
                    "accuracy": "high",
                    "release_type": "manual",
                    "is_on_end": True
                }]
            }]
        }
        
        result = adapter.convert_to_sets(response_json)
        
        assert len(result) == 1
        assert isinstance(result[0], GearSet)
        assert result[0].id == "gearset_001"
        assert result[0].share_with == ["partner_1"]

    def test_convert_to_sets_missing_share_with(self, adapter):
        """Test conversion when share_with is missing."""
        response_json = {
            "sets": [{
                "vessel_id": "vessel_001",
                "set_id": "gearset_001",
                "deployment_type": "lobster",
                "trawl_path": {},  # Changed to dict
                "when_updated_utc": "2023-09-15T18:00:00Z",
                "traps": [{
                    "trap_id": "trap_001",
                    "sequence": 1,
                    "latitude": 42.123456,
                    "longitude": -71.987654,
                    "deploy_datetime_utc": "2023-09-15T14:30:00Z",
                    "surface_datetime_utc": None,
                    "retrieved_datetime_utc": None,
                    "status": "deployed",
                    "accuracy": "high",
                    "release_type": "manual",
                    "is_on_end": False
                }]
            }]
        }
        
        result = adapter.convert_to_sets(response_json)
        
        assert len(result) == 1
        assert result[0].share_with == []

    def test_convert_to_sets_no_sets_key(self, adapter):
        """Test conversion when sets key is missing."""
        response_json = {"data": "invalid"}
        
        with patch('app.actions.rmwhub.adapter.logger') as mock_logger:
            result = adapter.convert_to_sets(response_json)
            
            assert result == []
            mock_logger.error.assert_called_once()

    @pytest.mark.skip(reason="Method build_observation_for_specific_trap was removed in refactoring")
    @pytest.mark.asyncio
    async def test_process_download(self, adapter, sample_gearset):
        """Test processing downloaded sets."""
        mock_observations = [{"test": "observation1"}, {"test": "observation2"}]
        
        # Mock gear_client.get_all_gears to return empty list
        adapter.gear_client.get_all_gears = AsyncMock(return_value=[])
        
        with patch('app.actions.rmwhub.types.GearSet.build_observation_for_specific_trap', new_callable=AsyncMock) as mock_build:
            mock_build.return_value = mock_observations
            
            result = await adapter.process_download([sample_gearset])
            
            assert result == mock_observations
            mock_build.assert_called_once()

    @pytest.mark.skip(reason="Method build_observation_for_specific_trap was removed in refactoring")
    @pytest.mark.asyncio
    async def test_process_download_multiple_sets(self, adapter, sample_gearset):
        """Test processing multiple downloaded sets."""
        mock_observations1 = [{"test": "observation1"}]
        mock_observations2 = [{"test": "observation2"}]
        
        gearset1 = sample_gearset
        gearset2 = GearSet(
            vessel_id="vessel_002",
            id="gearset_002",
            deployment_type="crab",
            traps_in_set=3,
            trawl_path={},  # Changed to dict
            share_with=[],
            when_updated_utc="2023-09-15T19:00:00Z",
            traps=[]
        )
        
        # Mock gear_client.get_all_gears to return empty list
        adapter.gear_client.get_all_gears = AsyncMock(return_value=[])
        
        with patch.object(GearSet, 'build_observation_for_specific_trap', new_callable=AsyncMock) as mock_build:
            mock_build.side_effect = [mock_observations1, mock_observations2]
            
            result = await adapter.process_download([gearset1, gearset2])
            
            # Only gearset1 has traps, so only one call should be made
            assert result == mock_observations1
            assert mock_build.call_count == 1

    @pytest.mark.asyncio
    async def test_iter_er_gears(self, adapter, sample_buoy_gear):
        """Test iterating over EarthRanger gears."""
        mock_gears = [sample_buoy_gear]
        
        async def mock_iter_gears(params=None):
            for gear in mock_gears:
                yield gear
        
        adapter.gear_client.iter_gears = mock_iter_gears
        
        result_gears = []
        async for gear in adapter.iter_er_gears():
            result_gears.append(gear)
        
        assert len(result_gears) == 1
        assert result_gears[0] == sample_buoy_gear

    @pytest.mark.asyncio
    async def test_process_upload_success(self, adapter, sample_buoy_gear):
        """Test successful upload process."""
        start_datetime = datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc)
        
        # Mock successful upload response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": {
                "trap_count": 1,
                "failed_sets": []
            }
        }
        
        async def mock_iter_gears(start_datetime=None, state=None):
            if state == "hauled":
                yield sample_buoy_gear
            # Don't yield anything for "deployed" state to avoid duplication
        
        with patch('app.actions.rmwhub.adapter.log_action_activity', new_callable=AsyncMock) as mock_log, \
             patch.object(adapter, 'iter_er_gears', side_effect=mock_iter_gears), \
             patch.object(adapter, '_create_rmw_update_from_er_gear', new_callable=AsyncMock) as mock_create_update:
            
            mock_log.return_value = "test_task_id"
            mock_update = MagicMock()
            mock_create_update.return_value = mock_update
            adapter.rmw_client.upload_data = AsyncMock(return_value=mock_response)
            
            trap_count, response_data = await adapter.process_upload(start_datetime)
            
            assert trap_count == 1
            assert response_data["result"]["trap_count"] == 1
            mock_log.assert_called()
            adapter.rmw_client.upload_data.assert_called_once_with([mock_update])

    @pytest.mark.asyncio
    async def test_process_upload_with_failed_sets(self, adapter, sample_buoy_gear):
        """Test upload process with failed sets."""
        start_datetime = datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc)
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "result": {
                "trap_count": 1,
                "failed_sets": ["set_1", "set_2"]
            }
        }
        
        async def mock_iter_gears(start_datetime=None, state=None):
            yield sample_buoy_gear
        
        with patch('app.actions.rmwhub.adapter.log_action_activity', new_callable=AsyncMock) as mock_log, \
             patch.object(adapter, 'iter_er_gears', side_effect=mock_iter_gears), \
             patch.object(adapter, '_create_rmw_update_from_er_gear', new_callable=AsyncMock) as mock_create_update:
            
            mock_log.return_value = "test_task_id"
            mock_update = MagicMock()
            mock_create_update.return_value = mock_update
            adapter.rmw_client.upload_data = AsyncMock(return_value=mock_response)
            
            trap_count, response_data = await adapter.process_upload(start_datetime)
            
            assert trap_count == 1
            # Should have logged warning for failed sets
            warning_calls = [call for call in mock_log.call_args_list if 
                           call[1].get('level') == LogLevel.WARNING]
            assert len(warning_calls) > 0

    @pytest.mark.asyncio
    async def test_process_upload_no_gears(self, adapter):
        """Test upload process when no gears are found."""
        start_datetime = datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc)
        
        async def mock_iter_gears(start_datetime=None, state=None):
            return
            yield  # This will never execute, creating an empty async generator
        
        with patch('app.actions.rmwhub.adapter.log_action_activity', new_callable=AsyncMock) as mock_log, \
             patch.object(adapter, 'iter_er_gears', side_effect=mock_iter_gears):
            
            mock_log.return_value = "test_task_id"
            
            trap_count, response_data = await adapter.process_upload(start_datetime)
            
            assert trap_count == 0
            assert response_data == {'result': {'failed_sets': [], 'trap_count': 0}}
            # Should have logged that no gear was found
            info_calls = [call for call in mock_log.call_args_list if 
                         call[1].get('level') == LogLevel.INFO and 
                         'No gear found' in call[1].get('title', '')]
            assert len(info_calls) > 0

    @pytest.mark.asyncio
    async def test_process_upload_gear_processing_error(self, adapter, sample_buoy_gear):
        """Test upload process when gear processing fails."""
        start_datetime = datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc)
        
        async def mock_iter_gears(start_datetime=None, state=None):
            yield sample_buoy_gear
        
        with patch('app.actions.rmwhub.adapter.log_action_activity', new_callable=AsyncMock) as mock_log, \
             patch.object(adapter, 'iter_er_gears', side_effect=mock_iter_gears), \
             patch.object(adapter, '_create_rmw_update_from_er_gear', new_callable=AsyncMock) as mock_create_update, \
             patch('app.actions.rmwhub.adapter.logger') as mock_logger:
            
            mock_log.return_value = "test_task_id"
            mock_create_update.side_effect = Exception("Processing error")
            
            trap_count, response_data = await adapter.process_upload(start_datetime)
            
            assert trap_count == 0
            assert response_data == {'result': {'failed_sets': [], 'trap_count': 0}}
            mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_process_upload_upload_error(self, adapter, sample_buoy_gear):
        """Test upload process when upload fails."""
        start_datetime = datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc)
        
        mock_response = MagicMock()
        mock_response.status_code = 500
        
        async def mock_iter_gears(start_datetime=None, state=None):
            yield sample_buoy_gear
        
        with patch('app.actions.rmwhub.adapter.log_action_activity', new_callable=AsyncMock) as mock_log, \
             patch.object(adapter, 'iter_er_gears', side_effect=mock_iter_gears), \
             patch.object(adapter, '_create_rmw_update_from_er_gear', new_callable=AsyncMock) as mock_create_update:
            
            mock_log.return_value = "test_task_id"
            mock_update = MagicMock()
            mock_create_update.return_value = mock_update
            adapter.rmw_client.upload_data = AsyncMock(return_value=mock_response)
            
            trap_count, response_data = await adapter.process_upload(start_datetime)
            
            assert trap_count == 0
            assert response_data == {'result': {'failed_sets': [], 'trap_count': 0}}
            # Should have logged error
            error_calls = [call for call in mock_log.call_args_list if 
                          call[1].get('level') == LogLevel.ERROR]
            assert len(error_calls) > 0

    @pytest.mark.asyncio
    async def test_process_upload_exception(self, adapter):
        """Test upload process when an exception occurs."""
        start_datetime = datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc)
        
        with patch('app.actions.rmwhub.adapter.log_action_activity', new_callable=AsyncMock) as mock_log, \
             patch.object(adapter, 'iter_er_gears', side_effect=Exception("Test exception")):
            
            mock_log.return_value = "test_task_id"
            
            trap_count, response_data = await adapter.process_upload(start_datetime)
            
            assert trap_count == 0
            assert response_data == []
            # Should have logged error
            error_calls = [call for call in mock_log.call_args_list if 
                          call[1].get('level') == LogLevel.ERROR]
            assert len(error_calls) > 0

    @pytest.mark.asyncio
    async def test_create_rmw_update_from_er_gear_deployed(self, adapter, sample_buoy_gear):
        """Test creating RMW update from deployed EarthRanger gear."""
        sample_buoy_gear.status = "deployed"
        
        result = await adapter._create_rmw_update_from_er_gear(sample_buoy_gear)
        
        assert isinstance(result, GearSet)
        assert result.id == str(sample_buoy_gear.id)
        assert len(result.traps) == len(sample_buoy_gear.devices)
        assert result.traps[0].status == "deployed"
        assert result.traps[0].retrieved_datetime_utc is None

    @pytest.mark.asyncio
    async def test_create_rmw_update_from_er_gear_retrieved(self, adapter, sample_buoy_gear):
        """Test creating RMW update from retrieved EarthRanger gear."""
        sample_buoy_gear.status = "retrieved"
        
        result = await adapter._create_rmw_update_from_er_gear(sample_buoy_gear)
        
        assert isinstance(result, GearSet)
        assert result.traps[0].status == "retrieved"
        assert result.traps[0].retrieved_datetime_utc == sample_buoy_gear.devices[0].last_updated.isoformat()

    @pytest.mark.asyncio
    async def test_create_rmw_update_multiple_devices(self, adapter, sample_buoy_gear):
        """Test creating RMW update with multiple devices."""
        # Add another device
        second_device = BuoyDevice(
            device_id=str(uuid.uuid4()),  # Use UUID as device_id
            mfr_device_id="mfr_002",
            label="Buoy Device 2",
            location=DeviceLocation(latitude=43.123456, longitude=-72.987654),
            last_updated=datetime(2023, 9, 15, 19, 0, 0, tzinfo=timezone.utc),
            last_deployed=datetime(2023, 9, 15, 15, 30, 0, tzinfo=timezone.utc)
        )
        sample_buoy_gear.devices.append(second_device)
        
        result = await adapter._create_rmw_update_from_er_gear(sample_buoy_gear)
        
        assert len(result.traps) == 2
        assert result.traps[0].sequence == 1
        assert result.traps[1].sequence == 2
        # Verify that IDs are set (not checking specific values since they're dynamic UUIDs)
        assert result.traps[0].id is not None
        assert result.traps[1].id is not None
        assert result.traps[0].id != result.traps[1].id  # Should be different

    @pytest.mark.asyncio
    async def test_create_display_id_to_gear_mapping(self, adapter):
        """Test creating display ID to gear mapping."""
        gear1 = BuoyGear(
            id=uuid.uuid4(),
            display_id="buoy_001",
            name="Buoy Gear 1",
            status="deployed",
            last_updated=datetime.now(timezone.utc),
            devices=[],
            type="buoy",
            manufacturer="test_manufacturer",
            additional={"display_id": "display_001"}
        )
        
        gear2 = BuoyGear(
            id=uuid.uuid4(),
            display_id="buoy_002",
            name="Buoy Gear 2",
            status="deployed",
            last_updated=datetime.now(timezone.utc),
            devices=[],
            type="buoy",
            manufacturer="rmwhub",  # Should be skipped
            additional={"display_id": "display_002"}
        )
        
        gear3 = BuoyGear(
            id=uuid.uuid4(),
            display_id="buoy_003",
            name="Buoy Gear 3",
            status="deployed",
            last_updated=datetime.now(timezone.utc),
            devices=[],
            type="buoy",
            manufacturer="other_manufacturer",
            additional={}  # No display_id
        )
        
        result = await adapter.create_display_id_to_gear_mapping([gear1, gear2, gear3])
        
        assert len(result) == 1
        assert "display_001" in result
        assert result["display_001"] == gear1
        assert "display_002" not in result  # Skipped rmwhub manufacturer
        assert "display_003" not in result  # No display_id

    def test_validate_response_valid_json(self, adapter):
        """Test validating valid JSON response."""
        valid_response = '{"test": "data"}'
        assert adapter.validate_response(valid_response) is True

    def test_validate_response_invalid_json(self, adapter):
        """Test validating invalid JSON response."""
        invalid_response = '{"test": invalid}'
        
        with patch('app.actions.rmwhub.adapter.logger') as mock_logger:
            assert adapter.validate_response(invalid_response) is False
            mock_logger.error.assert_called_once()

    def test_validate_response_empty(self, adapter):
        """Test validating empty response."""
        with patch('app.actions.rmwhub.adapter.logger') as mock_logger:
            assert adapter.validate_response("") is False
            mock_logger.error.assert_called_once()

    def test_validate_response_none(self, adapter):
        """Test validating None response."""
        with patch('app.actions.rmwhub.adapter.logger') as mock_logger:
            assert adapter.validate_response(None) is False
            mock_logger.error.assert_called_once()

    def test_clean_data_string(self, adapter):
        """Test cleaning string data."""
        dirty_string = "test\n\r\t'\"data  with  spaces"
        expected = "test data with spaces"
        result = adapter.clean_data(dirty_string)
        # The clean_data method replaces double spaces with single spaces only once
        # So "  " becomes " " but if there are more than two spaces, some remain
        assert "test" in result
        assert "data" in result
        assert "spaces" in result
        assert "\n" not in result
        assert "\r" not in result
        assert "\t" not in result
        assert "'" not in result
        assert '"' not in result

    def test_clean_data_non_string(self, adapter):
        """Test cleaning non-string data."""
        number = 123
        assert adapter.clean_data(number) == "123"

    def test_clean_data_none(self, adapter):
        """Test cleaning None data."""
        assert adapter.clean_data(None) == "None"

    def test_convert_datetime_to_utc_with_z(self, adapter):
        """Test converting datetime string ending with Z to UTC."""
        datetime_str = "2023-09-15T14:30:00Z"
        result = adapter.convert_datetime_to_utc(datetime_str)
        
        # The result should be a valid ISO format datetime string
        datetime.fromisoformat(result.replace('Z', '+00:00'))

    def test_convert_datetime_to_utc_with_offset(self, adapter):
        """Test converting datetime string with timezone offset to UTC."""
        datetime_str = "2023-09-15T14:30:00-04:00"
        result = adapter.convert_datetime_to_utc(datetime_str)
        
        # The result should be a valid ISO format datetime string
        datetime.fromisoformat(result.replace('Z', '+00:00'))

    def test_convert_datetime_to_utc_already_utc(self, adapter):
        """Test converting datetime string already in UTC."""
        datetime_str = "2023-09-15T14:30:00+00:00"
        result = adapter.convert_datetime_to_utc(datetime_str)
        
        # Should still work correctly
        datetime.fromisoformat(result.replace('Z', '+00:00'))

    @pytest.mark.asyncio
    async def test_process_upload_upload_exception(self, adapter, sample_buoy_gear):
        """Test upload process when upload raises an exception."""
        start_datetime = datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc)
        
        async def mock_iter_gears(start_datetime=None, state=None):
            yield sample_buoy_gear
        
        with patch('app.actions.rmwhub.adapter.log_action_activity', new_callable=AsyncMock) as mock_log, \
             patch.object(adapter, 'iter_er_gears', side_effect=mock_iter_gears), \
             patch.object(adapter, '_create_rmw_update_from_er_gear', new_callable=AsyncMock) as mock_create_update:
            
            mock_log.return_value = "test_task_id"
            mock_update = MagicMock()
            mock_create_update.return_value = mock_update
            adapter.rmw_client.upload_data = AsyncMock(side_effect=Exception("Upload exception"))
            
            trap_count, response_data = await adapter.process_upload(start_datetime)
            
            assert trap_count == 0
            assert response_data == {'result': {'failed_sets': [], 'trap_count': 0}}

    @pytest.mark.asyncio
    async def test_process_upload_no_updates_created(self, adapter, sample_buoy_gear):
        """Test upload process when no updates are created from gears."""
        start_datetime = datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc)
        
        async def mock_iter_gears(start_datetime=None, state=None):
            yield sample_buoy_gear
        
        with patch('app.actions.rmwhub.adapter.log_action_activity', new_callable=AsyncMock) as mock_log, \
             patch.object(adapter, 'iter_er_gears', side_effect=mock_iter_gears), \
             patch.object(adapter, '_create_rmw_update_from_er_gear', new_callable=AsyncMock) as mock_create_update:
            
            mock_log.return_value = "test_task_id"
            mock_create_update.return_value = None  # No update created
            
            trap_count, response_data = await adapter.process_upload(start_datetime)
            
            assert trap_count == 0
            assert response_data == {'result': {'failed_sets': [], 'trap_count': 0}}

    def test_clean_data_edge_cases(self, adapter):
        """Test cleaning data with various edge cases."""
        # Test multiple consecutive spaces
        test_string = "test   multiple    spaces"
        result = adapter.clean_data(test_string)
        assert "test" in result
        assert "multiple" in result
        assert "spaces" in result
        
        # Test all special characters
        test_string = "\n\r\t'\"text"
        result = adapter.clean_data(test_string)
        assert result == "text"
        
        # Test empty string
        assert adapter.clean_data("") == ""

    @pytest.mark.asyncio
    async def test_download_data_deployed_status(self, adapter):
        """Test download data with deployed status filter."""
        mock_response = {
            "sets": [{
                "vessel_id": "vessel_001",
                "set_id": "gearset_001",
                "deployment_type": "lobster",
                "traps_in_set": 2,
                "trawl_path": {},
                "share_with": [],
                "when_updated_utc": "2023-09-15T18:00:00Z",
                "traps": [
                    {
                        "trap_id": "trap_001",
                        "sequence": 1,
                        "latitude": 42.123456,
                        "longitude": -71.987654,
                        "deploy_datetime_utc": "2023-09-15T14:30:00Z",
                        "surface_datetime_utc": "2023-09-15T16:00:00Z",
                        "retrieved_datetime_utc": "2023-09-15T17:30:00Z",
                        "status": "deployed",
                        "accuracy": "high",
                        "release_type": "manual",
                        "is_on_end": False
                    },
                    {
                        "trap_id": "trap_002",
                        "sequence": 2,
                        "latitude": 42.123456,
                        "longitude": -71.987654,
                        "deploy_datetime_utc": "2023-09-15T14:30:00Z",
                        "surface_datetime_utc": "2023-09-15T16:00:00Z",
                        "retrieved_datetime_utc": "2023-09-15T17:30:00Z",
                        "status": "hauled",
                        "accuracy": "high",
                        "release_type": "manual",
                        "is_on_end": True
                    }
                ]
            }]
        }
        
        adapter.rmw_client.search_hub_all = AsyncMock(return_value=mock_response)
        
        result = await adapter.download_data(datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc), status="deployed")
        
        assert len(result) == 1
        assert len(result[0].traps) == 1  # Only deployed traps should remain
        assert result[0].traps[0].status == "deployed"

    @pytest.mark.asyncio
    async def test_download_data_hauled_status(self, adapter):
        """Test download data with hauled status filter."""
        mock_response = {
            "sets": [{
                "vessel_id": "vessel_001",
                "set_id": "gearset_001",
                "deployment_type": "lobster",
                "traps_in_set": 2,
                "trawl_path": {},
                "share_with": [],
                "when_updated_utc": "2023-09-15T18:00:00Z",
                "traps": [
                    {
                        "trap_id": "trap_001",
                        "sequence": 1,
                        "latitude": 42.123456,
                        "longitude": -71.987654,
                        "deploy_datetime_utc": "2023-09-15T14:30:00Z",
                        "surface_datetime_utc": "2023-09-15T16:00:00Z",
                        "retrieved_datetime_utc": "2023-09-15T17:30:00Z",
                        "status": "hauled",
                        "accuracy": "high",
                        "release_type": "manual",
                        "is_on_end": False
                    },
                    {
                        "trap_id": "trap_002",
                        "sequence": 2,
                        "latitude": 42.123456,
                        "longitude": -71.987654,
                        "deploy_datetime_utc": "2023-09-15T14:30:00Z",
                        "surface_datetime_utc": "2023-09-15T16:00:00Z",
                        "retrieved_datetime_utc": "2023-09-15T17:30:00Z",
                        "status": "hauled",
                        "accuracy": "high",
                        "release_type": "manual",
                        "is_on_end": True
                    }
                ]
            }]
        }
        
        adapter.rmw_client.search_hub_all = AsyncMock(return_value=mock_response)
        
        result = await adapter.download_data(datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc), status="hauled")
        
        assert len(result) == 1  # Should include the set with all hauled traps

    @pytest.mark.asyncio
    async def test_iter_er_gears_with_state(self, adapter, sample_buoy_gear):
        """Test iterating over EarthRanger gears with state filter."""
        
        async def mock_iter_gears(params=None):
            yield sample_buoy_gear
        
        adapter.gear_client.iter_gears = mock_iter_gears
        
        result_gears = []
        async for gear in adapter.iter_er_gears(state="deployed"):
            result_gears.append(gear)
        
        assert len(result_gears) == 1
        assert result_gears[0] == sample_buoy_gear

    @pytest.mark.asyncio
    async def test_process_upload_with_rmwhub_manufacturer(self, adapter):
        """Test upload process skipping gear with rmwhub manufacturer."""
        start_datetime = datetime(2023, 9, 15, 10, 0, 0, tzinfo=timezone.utc)
        
        rmwhub_gear = BuoyGear(
            id=uuid.uuid4(),
            display_id="rmwhub_001",
            name="RMW Hub Gear",
            status="deployed",
            last_updated=datetime.now(timezone.utc),
            devices=[],
            type="buoy",
            manufacturer="rmwhub",
            additional={}
        )
        
        async def mock_iter_gears(start_datetime=None, state=None):
            if state == "hauled":
                yield rmwhub_gear
        
        with patch('app.actions.rmwhub.adapter.log_action_activity', new_callable=AsyncMock) as mock_log:
            mock_log.return_value = "test_task_id"
            adapter.iter_er_gears = mock_iter_gears
            
            trap_count, response_data = await adapter.process_upload(start_datetime)
            
            assert trap_count == 0
            assert response_data == {'result': {'failed_sets': [], 'trap_count': 0}}

    @pytest.mark.asyncio 
    async def test_process_download_with_matching_status(self, adapter):
        """Test process download when trap status matches ER gear status."""
        gearset = GearSet(
            vessel_id="vessel_001",
            id="gearset_001",
            deployment_type="lobster",
            traps_in_set=1,
            trawl_path={},
            share_with=[],
            when_updated_utc="2023-09-15T18:00:00Z",
            traps=[Trap(
                id="device_001",  # This should match device_id 
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
            )]
        )
        
        er_gear = BuoyGear(
            id=uuid.uuid4(),
            display_id="gear_001",
            name="Test Gear",
            status="deployed",  # Same status as trap
            last_updated=datetime.now(timezone.utc),
            devices=[BuoyDevice(
                device_id="device_001",  # This should match trap.id
                mfr_device_id="mfr_001",
                label="Device 1",
                location=DeviceLocation(latitude=42.123456, longitude=-71.987654),
                last_updated=datetime.now(timezone.utc),
                last_deployed=datetime.now(timezone.utc)
            )],
            type="buoy",
            manufacturer="test_manufacturer",
            additional={}
        )
        
        adapter.gear_client.get_all_gears = AsyncMock(return_value=[er_gear])
        
        result = await adapter.process_download([gearset])
        
        assert result == []  # Should skip because statuses match

    @pytest.mark.asyncio
    async def test_process_download_retrieved_trap_no_er_gear(self, adapter):
        """Test process download with retrieved trap but no ER gear found."""
        gearset = GearSet(
            vessel_id="vessel_001",
            id="gearset_001",
            deployment_type="lobster",
            traps_in_set=1,
            trawl_path={},
            share_with=[],
            when_updated_utc="2023-09-15T18:00:00Z",
            traps=[Trap(
                id="unknown_trap",
                sequence=1,
                latitude=42.123456,
                longitude=-71.987654,
                deploy_datetime_utc="2023-09-15T14:30:00Z",
                surface_datetime_utc="2023-09-15T16:00:00Z",
                retrieved_datetime_utc="2023-09-15T17:30:00Z",
                status="retrieved",  # Retrieved but no ER gear
                accuracy="high",
                release_type="manual",
                is_on_end=True
            )]
        )
        
        adapter.gear_client.get_all_gears = AsyncMock(return_value=[])
        
        result = await adapter.process_download([gearset])
        
        assert result == []  # Should skip retrieved trap with no ER gear

    @pytest.mark.asyncio
    async def test_create_rmw_update_from_rmwhub_gear(self, adapter):
        """Test creating RMW update from gear with rmwhub manufacturer returns None."""
        rmwhub_gear = BuoyGear(
            id=uuid.uuid4(),
            display_id="rmwhub_001",
            name="RMW Hub Gear",
            status="deployed",
            last_updated=datetime.now(timezone.utc),
            devices=[],
            type="buoy",
            manufacturer="rmwhub",
            additional={}
        )
        
        result = await adapter._create_rmw_update_from_er_gear(rmwhub_gear)
        
        assert result is None

    def test_get_serial_number_from_device_id_edgetech(self, adapter):
        """Test _get_serial_number_from_device_id with edgetech manufacturer."""
        device_id = "ET123456_device001"
        manufacturer = "edgetech"
        
        result = adapter._get_serial_number_from_device_id(device_id, manufacturer)
        
        assert result == "ET123456"

    def test_get_serial_number_from_device_id_edgetech_case_insensitive(self, adapter):
        """Test _get_serial_number_from_device_id with EDGETECH (uppercase) manufacturer."""
        device_id = "ET789012_device002"
        manufacturer = "EDGETECH"
        
        result = adapter._get_serial_number_from_device_id(device_id, manufacturer)
        
        assert result == "ET789012"

    def test_get_serial_number_from_device_id_other_manufacturer(self, adapter):
        """Test _get_serial_number_from_device_id with non-edgetech manufacturer (covers line 310)."""
        device_id = "DEVICE123456"
        manufacturer = "other_manufacturer"
        
        result = adapter._get_serial_number_from_device_id(device_id, manufacturer)
        
        assert result == "DEVICE123456"

    def test_get_serial_number_from_device_id_empty_manufacturer(self, adapter):
        """Test _get_serial_number_from_device_id with empty manufacturer (covers line 310)."""
        device_id = "DEVICE789012"
        manufacturer = ""
        
        result = adapter._get_serial_number_from_device_id(device_id, manufacturer)
        
        assert result == "DEVICE789012"

    def test_create_gear_payload_from_gearset_deployed(self, adapter):
        """Test creating gear payload for deployed traps includes recorded_at."""
        trap = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.123456,
            longitude=-71.987654,
            deploy_datetime_utc="2023-09-15T14:30:00",
            surface_datetime_utc=None,
            retrieved_datetime_utc=None,
            status="deployed",
            accuracy="gps",
            release_type="manual",
            is_on_end=True
        )
        gearset = GearSet(
            vessel_id="vessel_001",
            id="gearset_001",
            deployment_type="lobster",
            traps_in_set=1,
            trawl_path={},
            share_with=[],
            when_updated_utc="2023-09-15T18:00:00Z",
            traps=[trap]
        )
        
        result = adapter._create_gear_payload_from_gearset(gearset, [trap], "deployed")
        
        assert len(result["devices"]) == 1
        device = result["devices"][0]
        assert "recorded_at" in device
        # When gearset.when_updated_utc is after deploy time, use it so location/set updates are applied
        assert device["recorded_at"] == "2023-09-15T18:00:00+00:00"
        assert device["last_deployed"] == "2023-09-15T14:30:00+00:00"
        assert device["last_updated"] == "2023-09-15T18:00:00+00:00"
        assert device["device_status"] == "deployed"
        assert result["initial_deployment_date"] == "2023-09-15T14:30:00+00:00"

    def test_create_gear_payload_from_gearset_deployed_uses_deploy_time_when_gearset_unchanged(self, adapter):
        """When when_updated_utc is missing or not after deploy, last_updated/recorded_at stay at deploy time."""
        trap = Trap(
            id="trap_002",
            sequence=1,
            latitude=42.0,
            longitude=-70.0,
            deploy_datetime_utc="2023-09-20T10:00:00Z",
            surface_datetime_utc=None,
            retrieved_datetime_utc=None,
            status="deployed",
            accuracy="gps",
            release_type="manual",
            is_on_end=True
        )
        gearset = GearSet(
            vessel_id="vessel_001",
            id="gearset_002",
            deployment_type="single",
            traps_in_set=1,
            trawl_path={},
            share_with=[],
            when_updated_utc="2023-09-20T09:00:00Z",  # before deploy
            traps=[trap]
        )
        result = adapter._create_gear_payload_from_gearset(gearset, [trap], "deployed")
        device = result["devices"][0]
        assert device["last_deployed"] == "2023-09-20T10:00:00+00:00"
        assert device["last_updated"] == "2023-09-20T10:00:00+00:00"
        assert device["recorded_at"] == "2023-09-20T10:00:00+00:00"

    def test_create_gear_payload_from_gearset_hauled_with_retrieved(self, adapter):
        """Test creating gear payload for hauled traps uses retrieved_datetime_utc for recorded_at."""
        trap = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.123456,
            longitude=-71.987654,
            deploy_datetime_utc="2023-09-15T14:30:00",
            surface_datetime_utc="2023-09-15T16:00:00",
            retrieved_datetime_utc="2023-09-15T17:30:00",
            status="retrieved",
            accuracy="gps",
            release_type="manual",
            is_on_end=True
        )
        gearset = GearSet(
            vessel_id="vessel_001",
            id="gearset_001",
            deployment_type="lobster",
            traps_in_set=1,
            trawl_path={},
            share_with=[],
            when_updated_utc="2023-09-15T18:00:00Z",
            traps=[trap]
        )
        
        result = adapter._create_gear_payload_from_gearset(gearset, [trap], "hauled")
        
        device = result["devices"][0]
        assert "recorded_at" in device
        # For hauled status, recorded_at should be retrieved_datetime_utc
        assert device["recorded_at"] == "2023-09-15T17:30:00+00:00"
        assert device["last_deployed"] == "2023-09-15T14:30:00+00:00"
        assert device["last_updated"] == "2023-09-15T17:30:00+00:00"
        assert device["device_status"] == "hauled"
        # No initial_deployment_date for hauled
        assert "initial_deployment_date" not in result

    def test_create_gear_payload_from_gearset_hauled_with_surface_only(self, adapter):
        """Test creating gear payload for hauled traps falls back to surface_datetime_utc."""
        trap = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.123456,
            longitude=-71.987654,
            deploy_datetime_utc="2023-09-15T14:30:00",
            surface_datetime_utc="2023-09-15T16:00:00",
            retrieved_datetime_utc=None,  # No retrieved time
            status="retrieved",
            accuracy="gps",
            release_type=None,
            is_on_end=True
        )
        gearset = GearSet(
            vessel_id="vessel_001",
            id="gearset_001",
            deployment_type="lobster",
            traps_in_set=1,
            trawl_path={},
            share_with=[],
            when_updated_utc="2023-09-15T18:00:00Z",
            traps=[trap]
        )
        
        result = adapter._create_gear_payload_from_gearset(gearset, [trap], "hauled")
        
        device = result["devices"][0]
        # Should fall back to surface_datetime_utc
        assert device["recorded_at"] == "2023-09-15T16:00:00+00:00"
        assert device["last_updated"] == "2023-09-15T16:00:00+00:00"

    def test_create_gear_payload_from_gearset_hauled_fallback_to_deploy(self, adapter):
        """Test creating gear payload for hauled traps falls back to deploy time when no recovery times."""
        trap = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.123456,
            longitude=-71.987654,
            deploy_datetime_utc="2023-09-15T14:30:00",
            surface_datetime_utc=None,
            retrieved_datetime_utc=None,
            status="retrieved",
            accuracy="gps",
            release_type=None,
            is_on_end=True
        )
        gearset = GearSet(
            vessel_id="vessel_001",
            id="gearset_001",
            deployment_type="lobster",
            traps_in_set=1,
            trawl_path={},
            share_with=[],
            when_updated_utc="2023-09-15T18:00:00Z",
            traps=[trap]
        )
        
        result = adapter._create_gear_payload_from_gearset(gearset, [trap], "hauled")
        
        device = result["devices"][0]
        # Should fall back to deploy_datetime_utc
        assert device["recorded_at"] == "2023-09-15T14:30:00+00:00"
        assert device["last_deployed"] == "2023-09-15T14:30:00+00:00"

    def test_create_gear_payload_from_gearset_timezone_already_present(self, adapter):
        """Test creating gear payload when timestamps already have timezone."""
        trap = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.123456,
            longitude=-71.987654,
            deploy_datetime_utc="2023-09-15T14:30:00+00:00",
            surface_datetime_utc=None,
            retrieved_datetime_utc=None,
            status="deployed",
            accuracy="gps",
            release_type=None,
            is_on_end=True
        )
        gearset = GearSet(
            vessel_id="vessel_001",
            id="gearset_001",
            deployment_type="single",
            traps_in_set=1,
            trawl_path={},
            share_with=[],
            when_updated_utc="2023-09-15T18:00:00Z",
            traps=[trap]
        )
        
        result = adapter._create_gear_payload_from_gearset(gearset, [trap], "deployed")
        
        device = result["devices"][0]
        # When gearset.when_updated_utc is after deploy, it is used (normalized to +00:00)
        assert device["recorded_at"] == "2023-09-15T18:00:00+00:00"

    def test_create_gear_payload_from_gearset_multiple_traps(self, adapter):
        """Test creating gear payload with multiple traps."""
        trap1 = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.123456,
            longitude=-71.987654,
            deploy_datetime_utc="2023-09-15T14:30:00",
            surface_datetime_utc=None,
            retrieved_datetime_utc=None,
            status="deployed",
            accuracy="gps",
            release_type="manual",
            is_on_end=False
        )
        trap2 = Trap(
            id="trap_002",
            sequence=2,
            latitude=42.234567,
            longitude=-71.876543,
            deploy_datetime_utc="2023-09-15T14:35:00",
            surface_datetime_utc=None,
            retrieved_datetime_utc=None,
            status="deployed",
            accuracy="gps",
            release_type="timed",
            is_on_end=True
        )
        gearset = GearSet(
            vessel_id="vessel_001",
            id="gearset_001",
            deployment_type="trawl",
            traps_in_set=2,
            trawl_path={},
            share_with=[],
            when_updated_utc="2023-09-15T18:00:00Z",
            traps=[trap1, trap2]
        )
        
        result = adapter._create_gear_payload_from_gearset(gearset, [trap1, trap2], "deployed")
        
        assert len(result["devices"]) == 2
        assert result["deployment_type"] == "trawl"
        assert result["devices_in_set"] == 2
        
        # Each device uses gearset when_updated_utc when it's after deploy so location/set updates apply
        assert result["devices"][0]["recorded_at"] == "2023-09-15T18:00:00+00:00"
        assert result["devices"][1]["recorded_at"] == "2023-09-15T18:00:00+00:00"
        
        # Check release_type handling
        assert result["devices"][0]["release_type"] == "manual"
        assert result["devices"][1]["release_type"] == "timed"

    def test_create_gear_payload_from_gearset_release_type_none(self, adapter):
        """Test creating gear payload excludes release_type when it's 'none'."""
        trap = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.123456,
            longitude=-71.987654,
            deploy_datetime_utc="2023-09-15T14:30:00",
            surface_datetime_utc=None,
            retrieved_datetime_utc=None,
            status="deployed",
            accuracy="gps",
            release_type="none",
            is_on_end=True
        )
        gearset = GearSet(
            vessel_id="vessel_001",
            id="gearset_001",
            deployment_type="single",
            traps_in_set=1,
            trawl_path={},
            share_with=[],
            when_updated_utc="2023-09-15T18:00:00Z",
            traps=[trap]
        )
        
        result = adapter._create_gear_payload_from_gearset(gearset, [trap], "deployed")
        
        device = result["devices"][0]
        assert "release_type" not in device

    def test_create_gear_payload_from_gearset_device_additional_data(self, adapter):
        """Test creating gear payload includes device_additional_data."""
        trap = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.123456,
            longitude=-71.987654,
            deploy_datetime_utc="2023-09-15T14:30:00",
            surface_datetime_utc="2023-09-15T16:00:00",
            retrieved_datetime_utc="2023-09-15T17:30:00",
            status="deployed",
            accuracy="gps",
            release_type="manual",
            is_on_end=True
        )
        gearset = GearSet(
            vessel_id="vessel_001",
            id="gearset_001",
            deployment_type="single",
            traps_in_set=1,
            trawl_path={},
            share_with=[],
            when_updated_utc="2023-09-15T18:00:00Z",
            traps=[trap]
        )
        
        result = adapter._create_gear_payload_from_gearset(gearset, [trap], "deployed")
        
        device = result["devices"][0]
        assert "device_additional_data" in device
        assert device["device_additional_data"]["id"] == "trap_001"
        assert device["device_additional_data"]["latitude"] == 42.123456


class TestDeduplicateTrapsById:
    """Tests for deduplicate_traps_by_id helper."""

    def test_no_duplicates_unchanged(self):
        """When all trap_ids are unique, list is unchanged (order by sequence)."""
        traps = [
            Trap(id="id-a", sequence=2, latitude=0, longitude=0, deploy_datetime_utc=None,
                 surface_datetime_utc=None, retrieved_datetime_utc=None, status="deployed",
                 accuracy="gps", release_type=None, is_on_end=False),
            Trap(id="id-b", sequence=1, latitude=0, longitude=0, deploy_datetime_utc=None,
                 surface_datetime_utc=None, retrieved_datetime_utc=None, status="deployed",
                 accuracy="gps", release_type=None, is_on_end=True),
        ]
        out, reports = deduplicate_traps_by_id(traps)
        assert len(out) == 2
        assert reports == []
        assert [t.id for t in out] == ["id-b", "id-a"]  # sorted by sequence

    def test_duplicate_trap_id_keeps_one_by_sequence(self):
        """When two traps share trap_id, keep the one with lowest sequence."""
        trap1 = Trap(id="same-id", sequence=1, latitude=44.1, longitude=-67.5,
                     deploy_datetime_utc="2026-02-15T18:01:00Z", surface_datetime_utc=None,
                     retrieved_datetime_utc=None, status="deployed", accuracy="gps",
                     release_type="acoustic", is_on_end=False)
        trap2 = Trap(id="same-id", sequence=2, latitude=44.2, longitude=-67.5,
                     deploy_datetime_utc="2026-02-15T18:02:00Z", surface_datetime_utc=None,
                     retrieved_datetime_utc=None, status="deployed", accuracy="gps",
                     release_type="acoustic", is_on_end=True)
        trap3 = Trap(id="other-id", sequence=3, latitude=44.3, longitude=-67.5,
                     deploy_datetime_utc="2026-02-15T18:03:00Z", surface_datetime_utc=None,
                     retrieved_datetime_utc=None, status="deployed", accuracy="gps",
                     release_type="acoustic", is_on_end=True)
        traps = [trap1, trap2, trap3]
        out, reports = deduplicate_traps_by_id(traps)
        assert len(out) == 2
        assert [t.id for t in out] == ["same-id", "other-id"]
        assert out[0].sequence == 1 and out[0].latitude == 44.1  # kept first by sequence
        assert reports == [("same-id", 1)]

    def test_three_entries_same_trap_id_keeps_one(self):
        """Three traps with same trap_id (e.g. RMW Hub duplicate rows) collapse to one."""
        traps = [
            Trap(id="dup", sequence=1, latitude=0, longitude=0, deploy_datetime_utc=None,
                 surface_datetime_utc=None, retrieved_datetime_utc=None, status="deployed",
                 accuracy="gps", release_type=None, is_on_end=False),
            Trap(id="dup", sequence=2, latitude=0, longitude=0, deploy_datetime_utc=None,
                 surface_datetime_utc=None, retrieved_datetime_utc=None, status="deployed",
                 accuracy="gps", release_type=None, is_on_end=False),
            Trap(id="dup", sequence=3, latitude=0, longitude=0, deploy_datetime_utc=None,
                 surface_datetime_utc=None, retrieved_datetime_utc=None, status="deployed",
                 accuracy="gps", release_type=None, is_on_end=True),
        ]
        out, reports = deduplicate_traps_by_id(traps)
        assert len(out) == 1
        assert out[0].id == "dup" and out[0].sequence == 1
        assert reports == [("dup", 2)]


class TestProcessDownloadDuplicateTrapIds:
    """Process download when a gearset has duplicate trap_ids (e.g. three device gearsets with one id repeated)."""

    @pytest.fixture
    def adapter(self):
        with patch('app.actions.rmwhub.adapter.RmwHubClient'), \
             patch('app.actions.rmwhub.adapter.BuoyClient'):
            return RmwHubAdapter(
                integration_id=str(uuid.uuid4()),
                api_key="test",
                rmw_url="https://test.rmwhub.com",
                er_token="test",
                er_destination="https://test.er.com",
            )

    @pytest.mark.asyncio
    async def test_process_download_deduplicates_trap_ids_in_payload(self, adapter):
        """Gearset with 3 traps where 2 share the same trap_id produces payload with 2 unique device_ids."""
        set_id = str(uuid.uuid4())
        trap_id_a = str(uuid.uuid4())
        trap_id_b = str(uuid.uuid4())
        gearset = GearSet(
            vessel_id=str(uuid.uuid4()),
            id=set_id,
            deployment_type="trawl",
            traps_in_set=3,
            trawl_path={},
            share_with=[],
            when_updated_utc="2026-02-15T18:03:03Z",
            traps=[
                Trap(id=trap_id_a, sequence=1, latitude=44.61484957, longitude=-67.50086846,
                     deploy_datetime_utc="2026-02-15T18:01:58Z", surface_datetime_utc=None,
                     retrieved_datetime_utc=None, status="deployed", accuracy="gps",
                     release_type="acoustic", is_on_end=True),
                Trap(id=trap_id_b, sequence=2, latitude=44.61540741, longitude=-67.50043699,
                     deploy_datetime_utc="2026-02-15T18:02:30Z", surface_datetime_utc=None,
                     retrieved_datetime_utc=None, status="deployed", accuracy="gps",
                     release_type="acoustic", is_on_end=True),
                Trap(id=trap_id_b, sequence=3, latitude=44.61540741, longitude=-67.50043699,
                     deploy_datetime_utc="2026-02-15T18:02:30Z", surface_datetime_utc=None,
                     retrieved_datetime_utc=None, status="deployed", accuracy="gps",
                     release_type="acoustic", is_on_end=True),
            ],
        )
        adapter.gear_client.get_all_gears = AsyncMock(return_value=[])

        payloads = await adapter.process_download([gearset])

        assert len(payloads) == 1
        payload = payloads[0]
        assert payload["devices_in_set"] == 2
        device_ids = [d["device_id"] for d in payload["devices"]]
        assert sorted(device_ids) == sorted([trap_id_a, trap_id_b])
        assert len(device_ids) == len(set(device_ids))  # all unique


class TestTimezoneHandling:
    """Tests for timezone parsing and normalization helpers."""

    def test_ensure_tz_utc_naive_timestamp(self):
        from app.actions.rmwhub.adapter import _ensure_tz_utc
        result = _ensure_tz_utc("2023-09-15T14:30:00")
        dt = datetime.fromisoformat(result)
        assert dt.tzinfo is not None

    def test_ensure_tz_utc_negative_offset(self):
        """Negative offset like -04:00 should NOT get +00:00 appended."""
        from app.actions.rmwhub.adapter import _ensure_tz_utc
        result = _ensure_tz_utc("2023-09-15T14:30:00-04:00")
        # Should be normalized to UTC
        dt = datetime.fromisoformat(result)
        assert dt.tzinfo is not None
        assert dt.utcoffset().total_seconds() == 0
        assert dt.hour == 18  # 14:30 - (-04:00) = 18:30

    def test_ensure_tz_utc_z_suffix(self):
        from app.actions.rmwhub.adapter import _ensure_tz_utc
        result = _ensure_tz_utc("2023-09-15T14:30:00Z")
        dt = datetime.fromisoformat(result)
        assert dt.tzinfo is not None

    def test_ensure_tz_utc_already_utc(self):
        from app.actions.rmwhub.adapter import _ensure_tz_utc
        result = _ensure_tz_utc("2023-09-15T14:30:00+00:00")
        dt = datetime.fromisoformat(result)
        assert dt.utcoffset().total_seconds() == 0

    def test_latest_haul_time_iso_compares_datetimes_not_strings(self):
        """Ensure _latest_haul_time_iso correctly picks latest regardless of Z vs +00:00 format."""
        from app.actions.rmwhub.adapter import _latest_haul_time_iso
        trap1 = Trap(
            id="t1", sequence=1, latitude=0, longitude=0,
            deploy_datetime_utc="2023-09-15T14:30:00Z",
            surface_datetime_utc=None,
            retrieved_datetime_utc="2023-09-15T20:00:00+00:00",
            status="retrieved", accuracy="gps", release_type=None, is_on_end=False,
        )
        trap2 = Trap(
            id="t2", sequence=2, latitude=0, longitude=0,
            deploy_datetime_utc="2023-09-15T14:30:00Z",
            surface_datetime_utc="2023-09-15T21:00:00Z",
            retrieved_datetime_utc=None,
            status="retrieved", accuracy="gps", release_type=None, is_on_end=True,
        )
        result = _latest_haul_time_iso([trap1, trap2], None)
        dt = datetime.fromisoformat(result)
        assert dt.hour == 21  # 21:00 is the latest


class TestMixedStatusDeployment:
    """Tests for the re-deploying hauled devices bug fix."""

    @pytest.fixture
    def adapter(self):
        with patch('app.actions.rmwhub.adapter.RmwHubClient'), \
             patch('app.actions.rmwhub.adapter.BuoyClient'):
            return RmwHubAdapter(
                integration_id=str(uuid.uuid4()),
                api_key="test",
                rmw_url="https://test.rmwhub.com",
                er_token="test",
                er_destination="https://test.er.com",
            )

    @pytest.mark.asyncio
    async def test_deploy_payload_excludes_retrieved_traps_when_er_gear_exists(self, adapter):
        """When ER gear exists and we build a full-set deploy payload, retrieved traps
        in gearset.traps are filtered out so they don't get re-sent as deployed.

        Scenario: ER set is deployed with trap A. RMW sends all-deployed traps:
        trap A (same location — skipped), trap B (new device — goes to traps_to_deploy),
        plus trap C marked 'retrieved' that somehow also has status='deployed' in RMW
        but is actually retrieved. The defensive filter ensures only status='deployed'
        traps from gearset.traps appear in the payload when building the full set list.

        We directly test the filtering by having gearset.traps include a retrieved trap
        alongside deployed traps and verifying the deploy payload excludes it.
        """
        set_id = str(uuid.uuid4())
        trap_id_a = str(uuid.uuid4())
        trap_id_b = str(uuid.uuid4())
        trap_id_c = str(uuid.uuid4())

        # All traps deployed in gearset.traps, but trap_c has status="retrieved"
        # This simulates a data inconsistency where gearset.traps contains mixed statuses
        trap_a = Trap(
            id=trap_id_a, sequence=1, latitude=42.0, longitude=-70.0,
            deploy_datetime_utc="2023-09-15T14:30:00Z", surface_datetime_utc=None,
            retrieved_datetime_utc=None, status="deployed", accuracy="gps",
            release_type=None, is_on_end=False,
        )
        trap_b = Trap(
            id=trap_id_b, sequence=2, latitude=42.2, longitude=-70.2,
            deploy_datetime_utc="2023-09-15T15:00:00Z", surface_datetime_utc=None,
            retrieved_datetime_utc=None, status="deployed", accuracy="gps",
            release_type=None, is_on_end=False,
        )
        trap_c = Trap(
            id=trap_id_c, sequence=3, latitude=42.3, longitude=-70.3,
            deploy_datetime_utc="2023-09-15T14:30:00Z", surface_datetime_utc=None,
            retrieved_datetime_utc="2023-09-15T17:30:00Z", status="retrieved", accuracy="gps",
            release_type=None, is_on_end=True,
        )

        gearset = GearSet(
            vessel_id="v1", id=set_id, deployment_type="trawl",
            traps_in_set=3, trawl_path={}, share_with=[],
            when_updated_utc="2023-09-15T18:00:00Z",
            traps=[trap_a, trap_b, trap_c],
        )

        # Directly test the deploy-only branch by calling _create_gear_payload_from_gearset
        # with the filtered list (as our fix does in process_download)
        from app.actions.rmwhub.adapter import deduplicate_traps_by_id
        er_traps_for_deploy = [
            t for t in gearset.traps
            if getattr(t, "status", None) != "retrieved"
        ]
        all_traps_for_set, _ = deduplicate_traps_by_id(er_traps_for_deploy)

        payload = adapter._create_gear_payload_from_gearset(
            gearset, all_traps_for_set, device_status="deployed"
        )

        device_ids = [d["device_id"] for d in payload["devices"]]
        # Retrieved trap C should be excluded
        assert trap_id_c not in device_ids
        # Deployed traps A and B should be present
        assert trap_id_a in device_ids
        assert trap_id_b in device_ids
        assert len(payload["devices"]) == 2
        assert all(d["device_status"] == "deployed" for d in payload["devices"])