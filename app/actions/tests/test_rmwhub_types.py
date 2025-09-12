import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

from pydantic import ValidationError

from app.actions.rmwhub.types import (
    SOURCE_TYPE,
    SUBJECT_SUBTYPE,
    GEAR_DEPLOYED_EVENT,
    GEAR_RETRIEVED_EVENT,
    EPOCH,
    Trap,
    GearSet
)


class TestConstants:
    """Test cases for module constants."""
    
    def test_constants_values(self):
        """Test that constants have correct values."""
        assert SOURCE_TYPE == "ropeless_buoy"
        assert SUBJECT_SUBTYPE == "ropeless_buoy_gearset"
        assert GEAR_DEPLOYED_EVENT == "gear_deployed"
        assert GEAR_RETRIEVED_EVENT == "gear_retrieved"
        assert EPOCH == "1970-01-01T00:00:00+00:00"


class TestTrap:
    """Test cases for the Trap model."""
    
    @pytest.fixture
    def sample_trap_data(self):
        """Fixture for sample trap data."""
        return {
            "id": "trap_001",
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
        }
    
    def test_trap_creation_full_data(self, sample_trap_data):
        """Test Trap creation with all fields."""
        trap = Trap(**sample_trap_data)
        
        assert trap.id == "trap_001"
        assert trap.sequence == 1
        assert trap.latitude == 42.123456
        assert trap.longitude == -71.987654
        assert trap.deploy_datetime_utc == "2023-09-15T14:30:00Z"
        assert trap.surface_datetime_utc == "2023-09-15T16:00:00Z"
        assert trap.retrieved_datetime_utc == "2023-09-15T17:30:00Z"
        assert trap.status == "deployed"
        assert trap.accuracy == "high"
        assert trap.release_type == "manual"
        assert trap.is_on_end is True
    
    def test_trap_creation_with_none_values(self):
        """Test Trap creation with None optional values."""
        trap = Trap(
            id="trap_002",
            sequence=2,
            latitude=43.0,
            longitude=-72.0,
            deploy_datetime_utc=None,
            surface_datetime_utc=None,
            retrieved_datetime_utc=None,
            status="retrieved",
            accuracy="medium",
            release_type=None,
            is_on_end=False
        )
        
        assert trap.id == "trap_002"
        assert trap.deploy_datetime_utc is None
        assert trap.surface_datetime_utc is None
        assert trap.retrieved_datetime_utc is None
        assert trap.release_type is None
    
    def test_trap_getitem_method(self, sample_trap_data):
        """Test Trap __getitem__ method."""
        trap = Trap(**sample_trap_data)
        
        assert trap["id"] == "trap_001"
        assert trap["sequence"] == 1
        assert trap["latitude"] == 42.123456
        assert trap["status"] == "deployed"
    
    def test_trap_get_method(self, sample_trap_data):
        """Test Trap get method."""
        trap = Trap(**sample_trap_data)
        
        assert trap.get("id") == "trap_001"
        assert trap.get("sequence") == 1
        assert trap.get("longitude") == -71.987654
        assert trap.get("accuracy") == "high"
    
    def test_trap_get_nonexistent_attribute(self, sample_trap_data):
        """Test Trap get method with non-existent attribute."""
        trap = Trap(**sample_trap_data)
        
        with pytest.raises(AttributeError):
            trap.get("nonexistent_field")
    
    def test_trap_hash(self, sample_trap_data):
        """Test Trap __hash__ method."""
        trap1 = Trap(**sample_trap_data)
        trap2 = Trap(**sample_trap_data)
        trap3_data = sample_trap_data.copy()
        trap3_data["sequence"] = 2
        trap3 = Trap(**trap3_data)
        
        # Same traps should have same hash
        assert hash(trap1) == hash(trap2)
        
        # Different traps should have different hash
        assert hash(trap1) != hash(trap3)
    
    def test_trap_validation_errors(self):
        """Test Trap validation errors."""
        # Missing required fields
        with pytest.raises(ValidationError):
            Trap()
        
        with pytest.raises(ValidationError):
            Trap(id="trap_001")
        
        # Invalid types
        with pytest.raises(ValidationError):
            Trap(
                id="trap_001",
                sequence="invalid",
                latitude=42.0,
                longitude=-71.0,
                status="deployed",
                accuracy="high",
                is_on_end=True
            )
        
        with pytest.raises(ValidationError):
            Trap(
                id="trap_001",
                sequence=1,
                latitude="invalid",
                longitude=-71.0,
                status="deployed",
                accuracy="high",
                is_on_end=True
            )
    
    @patch('app.actions.rmwhub.types.datetime')
    def test_get_latest_update_time_deployed_status_with_deploy_time(self, mock_datetime):
        """Test get_latest_update_time for deployed status with deploy time."""
        mock_now = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = mock_now
        
        trap = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.0,
            longitude=-71.0,
            deploy_datetime_utc="2023-09-15T14:30:00Z",
            status="deployed",
            accuracy="high",
            is_on_end=True
        )
        
        result = trap.get_latest_update_time()
        expected = datetime(2023, 9, 15, 14, 30, 0, tzinfo=timezone.utc)
        assert result == expected
    
    @patch('app.actions.rmwhub.types.datetime')
    def test_get_latest_update_time_deployed_status_no_deploy_time(self, mock_datetime):
        """Test get_latest_update_time for deployed status without deploy time."""
        mock_now = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = mock_now
        
        trap = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.0,
            longitude=-71.0,
            deploy_datetime_utc=None,
            status="deployed",
            accuracy="high",
            is_on_end=True
        )
        
        # Should call convert_to_utc with datetime.now()
        with patch.object(Trap, 'convert_to_utc') as mock_convert:
            mock_convert.return_value = mock_now
            result = trap.get_latest_update_time()
            mock_convert.assert_called_once_with(mock_now)
            assert result == mock_now
    
    def test_get_latest_update_time_retrieved_status_with_retrieved_time(self):
        """Test get_latest_update_time for retrieved status with retrieved time."""
        trap = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.0,
            longitude=-71.0,
            deploy_datetime_utc="2023-09-15T14:30:00Z",
            surface_datetime_utc="2023-09-15T16:00:00Z",
            retrieved_datetime_utc="2023-09-15T17:30:00Z",
            status="retrieved",
            accuracy="high",
            is_on_end=True
        )
        
        result = trap.get_latest_update_time()
        expected = datetime(2023, 9, 15, 17, 30, 0, tzinfo=timezone.utc)
        assert result == expected
    
    def test_get_latest_update_time_retrieved_status_with_surface_time(self):
        """Test get_latest_update_time for retrieved status with surface time only."""
        trap = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.0,
            longitude=-71.0,
            deploy_datetime_utc="2023-09-15T14:30:00Z",
            surface_datetime_utc="2023-09-15T16:00:00Z",
            retrieved_datetime_utc=None,
            status="retrieved",
            accuracy="high",
            is_on_end=True
        )
        
        result = trap.get_latest_update_time()
        expected = datetime(2023, 9, 15, 16, 0, 0, tzinfo=timezone.utc)
        assert result == expected
    
    def test_get_latest_update_time_retrieved_status_with_deploy_time_only(self):
        """Test get_latest_update_time for retrieved status with deploy time only."""
        trap = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.0,
            longitude=-71.0,
            deploy_datetime_utc="2023-09-15T14:30:00Z",
            surface_datetime_utc=None,
            retrieved_datetime_utc=None,
            status="retrieved",
            accuracy="high",
            is_on_end=True
        )
        
        result = trap.get_latest_update_time()
        expected = datetime(2023, 9, 15, 14, 30, 0, tzinfo=timezone.utc)
        assert result == expected
    
    @patch('app.actions.rmwhub.types.datetime')
    def test_get_latest_update_time_retrieved_status_no_times(self, mock_datetime):
        """Test get_latest_update_time for retrieved status with no times."""
        mock_now = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = mock_now
        
        trap = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.0,
            longitude=-71.0,
            deploy_datetime_utc=None,
            surface_datetime_utc=None,
            retrieved_datetime_utc=None,
            status="retrieved",
            accuracy="high",
            is_on_end=True
        )
        
        with patch.object(Trap, 'convert_to_utc') as mock_convert:
            mock_convert.return_value = mock_now
            result = trap.get_latest_update_time()
            mock_convert.assert_called_once_with(mock_now)
            assert result == mock_now
    
    def test_convert_to_utc_valid_string(self):
        """Test convert_to_utc with valid datetime string."""
        datetime_str = "2023-09-15T14:30:00Z"
        result = Trap.convert_to_utc(datetime_str)
        expected = datetime(2023, 9, 15, 14, 30, 0, tzinfo=timezone.utc)
        assert result == expected
    
    def test_convert_to_utc_different_formats(self):
        """Test convert_to_utc with different datetime formats."""
        # ISO format
        result1 = Trap.convert_to_utc("2023-09-15T14:30:00")
        expected1 = datetime(2023, 9, 15, 14, 30, 0, tzinfo=timezone.utc)
        assert result1 == expected1
        
        # Different format
        result2 = Trap.convert_to_utc("2023-09-15 14:30:00")
        expected2 = datetime(2023, 9, 15, 14, 30, 0, tzinfo=timezone.utc)
        assert result2 == expected2
    
    @patch('app.actions.rmwhub.types.datetime')
    def test_get_latest_update_time_unknown_status(self, mock_datetime):
        """Test get_latest_update_time for unknown status (traptime remains None)."""
        mock_now = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = mock_now
        
        trap = Trap(
            id="trap_001",
            sequence=1,
            latitude=42.0,
            longitude=-71.0,
            deploy_datetime_utc=None,
            status="unknown_status",  # This will leave traptime as None
            accuracy="high",
            is_on_end=True
        )
        
        # This should fail because traptime is None and convert_to_utc expects a string
        with pytest.raises(TypeError):
            trap.get_latest_update_time()
    
    @patch('app.actions.rmwhub.types.parse_date')
    def test_convert_to_utc_parse_failure(self, mock_parse_date):
        """Test convert_to_utc when parsing fails."""
        mock_parse_date.return_value = None
        
        with pytest.raises(AttributeError):
            Trap.convert_to_utc("invalid_date")
    
    @patch('app.actions.rmwhub.types.parse_date')
    def test_convert_to_utc_value_error_branch(self, mock_parse_date):
        """Test convert_to_utc ValueError branch - though this may be unreachable."""
        # Create a mock datetime object that evaluates to False
        mock_datetime = MagicMock()
        mock_datetime.__bool__ = MagicMock(return_value=False)
        mock_datetime.replace.return_value = None  # This makes utc_datetime_obj None/False
        mock_parse_date.return_value = mock_datetime
        
        with pytest.raises(ValueError, match="Unable to parse datetime string"):
            Trap.convert_to_utc("test_string")


class TestGearSet:
    """Test cases for the GearSet model."""
    
    @pytest.fixture
    def sample_trap_data_list(self):
        """Fixture for sample trap data list."""
        return [
            {
                "id": "trap_001",
                "sequence": 1,
                "latitude": 42.123456,
                "longitude": -71.987654,
                "deploy_datetime_utc": "2023-09-15T14:30:00Z",
                "surface_datetime_utc": "2023-09-15T16:00:00Z",
                "retrieved_datetime_utc": None,
                "status": "deployed",
                "accuracy": "high",
                "release_type": "manual",
                "is_on_end": True
            },
            {
                "id": "trap_002",
                "sequence": 2,
                "latitude": 43.0,
                "longitude": -72.0,
                "deploy_datetime_utc": "2023-09-15T14:35:00Z",
                "surface_datetime_utc": None,
                "retrieved_datetime_utc": "2023-09-15T18:00:00Z",
                "status": "retrieved",
                "accuracy": "medium",
                "release_type": "automatic",
                "is_on_end": False
            }
        ]
    
    @pytest.fixture
    def sample_gearset_data(self, sample_trap_data_list):
        """Fixture for sample GearSet data."""
        return {
            "vessel_id": "vessel_001",
            "id": "gearset_001",
            "deployment_type": "trawl",
            "traps_in_set": 2,
            "trawl_path": "path_001",
            "share_with": ["partner_001", "partner_002"],
            "traps": [Trap(**trap_data) for trap_data in sample_trap_data_list],
            "when_updated_utc": "2023-09-15T19:00:00Z"
        }
    
    def test_gearset_creation_full_data(self, sample_gearset_data):
        """Test GearSet creation with all fields."""
        gearset = GearSet(**sample_gearset_data)
        
        assert gearset.vessel_id == "vessel_001"
        assert gearset.id == "gearset_001"
        assert gearset.deployment_type == "trawl"
        assert gearset.traps_in_set == 2
        assert gearset.trawl_path == "path_001"
        assert gearset.share_with == ["partner_001", "partner_002"]
        assert len(gearset.traps) == 2
        assert gearset.when_updated_utc == "2023-09-15T19:00:00Z"
    
    def test_gearset_creation_with_none_values(self, sample_trap_data_list):
        """Test GearSet creation with None optional values."""
        gearset = GearSet(
            vessel_id="vessel_002",
            id="gearset_002",
            deployment_type="longline",
            traps_in_set=None,
            trawl_path=None,
            share_with=None,
            traps=[Trap(**sample_trap_data_list[0])],
            when_updated_utc="2023-09-15T20:00:00Z"
        )
        
        assert gearset.vessel_id == "vessel_002"
        assert gearset.traps_in_set is None
        assert gearset.trawl_path == ""  # Validator converts None to ""
        assert gearset.share_with == []  # Validator converts None to []
    
    def test_gearset_validator_none_to_empty_trawl_path(self):
        """Test trawl_path validator converts None to empty string."""
        gearset = GearSet(
            vessel_id="vessel_003",
            id="gearset_003",
            deployment_type="test",
            trawl_path=None,
            traps=[],
            when_updated_utc="2023-09-15T21:00:00Z"
        )
        
        assert gearset.trawl_path == ""
    
    def test_gearset_validator_none_to_empty_list_share_with(self):
        """Test share_with validator converts None to empty list."""
        gearset = GearSet(
            vessel_id="vessel_004",
            id="gearset_004",
            deployment_type="test",
            trawl_path="test_path",
            share_with=None,
            traps=[],
            when_updated_utc="2023-09-15T22:00:00Z"
        )
        
        assert gearset.share_with == []
    
    def test_gearset_validator_preserves_valid_values(self):
        """Test validators preserve valid values."""
        gearset = GearSet(
            vessel_id="vessel_005",
            id="gearset_005",
            deployment_type="test",
            trawl_path="valid_path",
            share_with=["partner1"],
            traps=[],
            when_updated_utc="2023-09-15T23:00:00Z"
        )
        
        assert gearset.trawl_path == "valid_path"
        assert gearset.share_with == ["partner1"]
    
    def test_gearset_getitem_method(self, sample_gearset_data):
        """Test GearSet __getitem__ method."""
        gearset = GearSet(**sample_gearset_data)
        
        assert gearset["vessel_id"] == "vessel_001"
        assert gearset["id"] == "gearset_001"
        assert gearset["deployment_type"] == "trawl"
        assert gearset["traps_in_set"] == 2
    
    def test_gearset_get_method(self, sample_gearset_data):
        """Test GearSet get method."""
        gearset = GearSet(**sample_gearset_data)
        
        assert gearset.get("vessel_id") == "vessel_001"
        assert gearset.get("trawl_path") == "path_001"
        assert gearset.get("share_with") == ["partner_001", "partner_002"]
        assert gearset.get("when_updated_utc") == "2023-09-15T19:00:00Z"
    
    def test_gearset_get_nonexistent_attribute(self, sample_gearset_data):
        """Test GearSet get method with non-existent attribute."""
        gearset = GearSet(**sample_gearset_data)
        
        with pytest.raises(AttributeError):
            gearset.get("nonexistent_field")
    
    def test_gearset_hash(self, sample_gearset_data):
        """Test GearSet __hash__ method."""
        gearset1 = GearSet(**sample_gearset_data)
        gearset2 = GearSet(**sample_gearset_data)
        
        # Same gearsets should have same hash
        assert hash(gearset1) == hash(gearset2)
        
        # Different gearsets should have different hash
        modified_data = sample_gearset_data.copy()
        modified_data["id"] = "different_id"
        gearset3 = GearSet(**modified_data)
        assert hash(gearset1) != hash(gearset3)
    
    def test_gearset_validation_errors(self):
        """Test GearSet validation errors."""
        # Missing required fields
        with pytest.raises(ValidationError):
            GearSet()
        
        with pytest.raises(ValidationError):
            GearSet(vessel_id="vessel_001")
        
        # Invalid types
        with pytest.raises(ValidationError):
            GearSet(
                vessel_id="vessel_001",
                id="gearset_001",
                deployment_type="test",
                traps="invalid_type",  # Should be List[Trap]
                when_updated_utc="2023-09-15T19:00:00Z"
            )
    
    @pytest.mark.asyncio
    async def test_build_observations_empty_traps(self):
        """Test build_observations with empty traps list."""
        gearset = GearSet(
            vessel_id="vessel_empty",
            id="gearset_empty",
            deployment_type="test",
            trawl_path="",
            traps=[],
            when_updated_utc="2023-09-15T19:00:00Z"
        )
        
        observations = await gearset.build_observations()
        assert observations == []
    
    @pytest.mark.asyncio
    async def test_build_observations_deployed_trap(self):
        """Test build_observations with deployed trap."""
        trap = Trap(
            id="trap_deployed",
            sequence=1,
            latitude=42.123456,
            longitude=-71.987654,
            deploy_datetime_utc="2023-09-15T14:30:00Z",
            status="deployed",
            accuracy="high",
            is_on_end=True
        )
        
        gearset = GearSet(
            vessel_id="vessel_deployed",
            id="gearset_deployed",
            deployment_type="test",
            trawl_path="",
            traps=[trap],
            when_updated_utc="2023-09-15T19:00:00Z"
        )
        
        observations = await gearset.build_observations()
        
        assert len(observations) == 1
        obs = observations[0]
        
        assert obs["location"]["lat"] == 42.123456
        assert obs["location"]["lon"] == -71.987654
        assert obs["recorded_at"] == datetime(2023, 9, 15, 14, 30, 0, tzinfo=timezone.utc)
        assert obs["type"] == SOURCE_TYPE
        assert obs["subject_type"] == SUBJECT_SUBTYPE
        assert obs["source_name"] == "gearset_deployed"
        assert obs["source"] == "trap_deployed"
        assert obs["additional"]["event_type"] == "gear_deployed"
    
    @pytest.mark.asyncio
    async def test_build_observations_retrieved_trap(self):
        """Test build_observations with retrieved trap."""
        trap = Trap(
            id="trap_retrieved",
            sequence=1,
            latitude=43.0,
            longitude=-72.0,
            deploy_datetime_utc="2023-09-15T14:30:00Z",
            retrieved_datetime_utc="2023-09-15T18:00:00Z",
            status="retrieved",
            accuracy="medium",
            is_on_end=False
        )
        
        gearset = GearSet(
            vessel_id="vessel_retrieved",
            id="gearset_retrieved",
            deployment_type="test",
            trawl_path="",
            traps=[trap],
            when_updated_utc="2023-09-15T19:00:00Z"
        )
        
        observations = await gearset.build_observations()
        
        assert len(observations) == 1
        obs = observations[0]
        
        assert obs["location"]["lat"] == 43.0
        assert obs["location"]["lon"] == -72.0
        assert obs["recorded_at"] == datetime(2023, 9, 15, 18, 0, 0, tzinfo=timezone.utc)
        assert obs["type"] == SOURCE_TYPE
        assert obs["subject_type"] == SUBJECT_SUBTYPE
        assert obs["source_name"] == "gearset_retrieved"
        assert obs["source"] == "trap_retrieved"
        assert obs["additional"]["event_type"] == "gear_retrieved"
    
    @pytest.mark.asyncio
    async def test_build_observations_multiple_traps(self, sample_trap_data_list):
        """Test build_observations with multiple traps."""
        traps = [Trap(**trap_data) for trap_data in sample_trap_data_list]
        
        gearset = GearSet(
            vessel_id="vessel_multi",
            id="gearset_multi",
            deployment_type="test",
            trawl_path="",
            traps=traps,
            when_updated_utc="2023-09-15T19:00:00Z"
        )
        
        observations = await gearset.build_observations()
        
        assert len(observations) == 2
        
        # Check first observation (deployed)
        obs1 = observations[0]
        assert obs1["location"]["lat"] == 42.123456
        assert obs1["location"]["lon"] == -71.987654
        assert obs1["source_name"] == "gearset_multi"
        assert obs1["source"] == "trap_001"
        assert obs1["additional"]["event_type"] == "gear_deployed"
        
        # Check second observation (retrieved)
        obs2 = observations[1]
        assert obs2["location"]["lat"] == 43.0
        assert obs2["location"]["lon"] == -72.0
        assert obs2["source_name"] == "gearset_multi"
        assert obs2["source"] == "trap_002"
        assert obs2["additional"]["event_type"] == "gear_retrieved"
    
    @pytest.mark.asyncio
    async def test_build_observations_unknown_status(self):
        """Test build_observations with unknown trap status."""
        # Mock the get_latest_update_time to avoid the TypeError from parse_date
        with patch.object(Trap, 'get_latest_update_time') as mock_get_time:
            mock_get_time.return_value = datetime(2023, 9, 15, 14, 30, 0, tzinfo=timezone.utc)
            
            trap = Trap(
                id="trap_unknown",
                sequence=1,
                latitude=42.0,
                longitude=-71.0,
                deploy_datetime_utc="2023-09-15T14:30:00Z",
                status="unknown_status",
                accuracy="high",
                is_on_end=True
            )
            
            gearset = GearSet(
                vessel_id="vessel_unknown",
                id="gearset_unknown",
                deployment_type="test",
                trawl_path="",
                traps=[trap],
                when_updated_utc="2023-09-15T19:00:00Z"
            )
            
            with pytest.raises(ValueError, match="Unknown trap status: unknown_status"):
                await gearset.build_observations()
    
    @pytest.mark.asyncio
    async def test_build_observations_trap_with_unknown_status_and_get_latest_update_time(self):
        """Test that a trap with unknown status still processes time correctly before status check."""
        trap = Trap(
            id="trap_unknown_status",
            sequence=1,
            latitude=42.0,
            longitude=-71.0,
            deploy_datetime_utc="2023-09-15T14:30:00Z",
            status="unknown_status",
            accuracy="high",
            is_on_end=True
        )
        
        # Test the get_latest_update_time method directly for unknown status
        with patch('app.actions.rmwhub.types.datetime') as mock_datetime:
            mock_now = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
            mock_datetime.now.return_value = mock_now
            
            # This should call convert_to_utc with None since status doesn't match "deployed" or "retrieved"
            with pytest.raises(TypeError):  # parse_date will fail with None
                trap.get_latest_update_time()
    
    def test_convert_to_utc_with_datetime_object(self):
        """Test convert_to_utc when passed a datetime object instead of string."""
        dt_obj = datetime(2023, 9, 15, 14, 30, 0, tzinfo=timezone.utc)
        
        # This should fail because parse_date expects a string, not a datetime object
        with pytest.raises(TypeError):
            Trap.convert_to_utc(dt_obj)
    
    def test_convert_to_utc_unreachable_code_coverage(self):
        """Test the unreachable ValueError in convert_to_utc."""
        # The line "if not utc_datetime_obj:" is unreachable because 
        # parse_date either returns a datetime object or None/raises exception
        # This test exists for documentation but cannot reach that line
        pass