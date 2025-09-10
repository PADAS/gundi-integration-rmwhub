import pytest
from datetime import datetime, timezone
from uuid import uuid4

from pydantic import ValidationError

from app.actions.buoy.types import Environment, DeviceLocation, BuoyDevice, BuoyGear


class TestEnvironment:
    """Test cases for the Environment enum."""
    
    def test_environment_values(self):
        """Test that Environment enum has correct values."""
        assert Environment.DEV.value == "Buoy Dev"
        assert Environment.STAGE.value == "Buoy Staging"
        assert Environment.PRODUCTION.value == "Buoy Prod"
    
    def test_environment_names(self):
        """Test that Environment enum has correct names."""
        assert Environment.DEV.name == "DEV"
        assert Environment.STAGE.name == "STAGE"
        assert Environment.PRODUCTION.name == "PRODUCTION"
    
    def test_environment_from_value(self):
        """Test creating Environment from values."""
        assert Environment("Buoy Dev") == Environment.DEV
        assert Environment("Buoy Staging") == Environment.STAGE
        assert Environment("Buoy Prod") == Environment.PRODUCTION


class TestDeviceLocation:
    """Test cases for the DeviceLocation model."""
    
    def test_device_location_creation(self):
        """Test successful DeviceLocation creation."""
        location = DeviceLocation(latitude=42.123456, longitude=-71.987654)
        
        assert location.latitude == 42.123456
        assert location.longitude == -71.987654
    
    def test_device_location_validation(self):
        """Test DeviceLocation validation."""
        # Valid data
        location = DeviceLocation(latitude=0.0, longitude=0.0)
        assert location.latitude == 0.0
        assert location.longitude == 0.0
        
        # Test with extreme valid values
        location = DeviceLocation(latitude=90.0, longitude=180.0)
        assert location.latitude == 90.0
        assert location.longitude == 180.0
        
        location = DeviceLocation(latitude=-90.0, longitude=-180.0)
        assert location.latitude == -90.0
        assert location.longitude == -180.0
    
    def test_device_location_invalid_types(self):
        """Test DeviceLocation with invalid types."""
        with pytest.raises(ValidationError):
            DeviceLocation(latitude="invalid", longitude=0.0)
        
        with pytest.raises(ValidationError):
            DeviceLocation(latitude=0.0, longitude="invalid")
        
        with pytest.raises(ValidationError):
            DeviceLocation(latitude=None, longitude=0.0)
    
    def test_device_location_missing_fields(self):
        """Test DeviceLocation with missing required fields."""
        with pytest.raises(ValidationError):
            DeviceLocation(latitude=42.123456)
        
        with pytest.raises(ValidationError):
            DeviceLocation(longitude=-71.987654)
        
        with pytest.raises(ValidationError):
            DeviceLocation()


class TestBuoyDevice:
    """Test cases for the BuoyDevice model."""
    
    @pytest.fixture
    def sample_location(self):
        """Fixture for a sample DeviceLocation."""
        return DeviceLocation(latitude=42.123456, longitude=-71.987654)
    
    @pytest.fixture
    def sample_datetime(self):
        """Fixture for a sample datetime."""
        return datetime(2023, 9, 15, 14, 30, 0, tzinfo=timezone.utc)
    
    def test_buoy_device_creation_with_deployment(self, sample_location, sample_datetime):
        """Test successful BuoyDevice creation with deployment date."""
        device = BuoyDevice(
            device_id="device_001",
            label="Test Buoy Device",
            location=sample_location,
            last_updated=sample_datetime,
            last_deployed=sample_datetime
        )
        
        assert device.device_id == "device_001"
        assert device.label == "Test Buoy Device"
        assert device.location == sample_location
        assert device.last_updated == sample_datetime
        assert device.last_deployed == sample_datetime
    
    def test_buoy_device_creation_without_deployment(self, sample_location, sample_datetime):
        """Test successful BuoyDevice creation without deployment date."""
        device = BuoyDevice(
            device_id="device_002",
            label="Undeployed Device",
            location=sample_location,
            last_updated=sample_datetime,
            last_deployed=None
        )
        
        assert device.device_id == "device_002"
        assert device.label == "Undeployed Device"
        assert device.location == sample_location
        assert device.last_updated == sample_datetime
        assert device.last_deployed is None
    
    def test_buoy_device_optional_deployment_default(self, sample_location, sample_datetime):
        """Test BuoyDevice with default None for last_deployed."""
        device = BuoyDevice(
            device_id="device_003",
            label="Default Device",
            location=sample_location,
            last_updated=sample_datetime
        )
        
        assert device.last_deployed is None
    
    def test_buoy_device_validation_errors(self, sample_location, sample_datetime):
        """Test BuoyDevice validation errors."""
        # Missing required fields
        with pytest.raises(ValidationError):
            BuoyDevice()
        
        with pytest.raises(ValidationError):
            BuoyDevice(device_id="device_001")
        
        with pytest.raises(ValidationError):
            BuoyDevice(
                device_id="device_001",
                label="Test Device"
            )
        
        # Invalid datetime type
        with pytest.raises(ValidationError):
            BuoyDevice(
                device_id="device_001",
                label="Test Device",
                location=sample_location,
                last_updated="invalid_date"
            )
        
        # Invalid location type
        with pytest.raises(ValidationError):
            BuoyDevice(
                device_id="device_001",
                label="Test Device",
                location="invalid_location",
                last_updated=sample_datetime
            )


class TestBuoyGear:
    """Test cases for the BuoyGear model."""
    
    @pytest.fixture
    def sample_location(self):
        """Fixture for a sample DeviceLocation."""
        return DeviceLocation(latitude=42.123456, longitude=-71.987654)
    
    @pytest.fixture
    def sample_datetime(self):
        """Fixture for a sample datetime."""
        return datetime(2023, 9, 15, 14, 30, 0, tzinfo=timezone.utc)
    
    @pytest.fixture
    def sample_devices(self, sample_location, sample_datetime):
        """Fixture for sample BuoyDevice list."""
        return [
            BuoyDevice(
                device_id="device_001",
                label="Device 1",
                location=sample_location,
                last_updated=sample_datetime,
                last_deployed=sample_datetime
            ),
            BuoyDevice(
                device_id="device_002",
                label="Device 2",
                location=DeviceLocation(latitude=43.0, longitude=-72.0),
                last_updated=sample_datetime,
                last_deployed=None
            )
        ]
    
    @pytest.fixture
    def sample_gear_id(self):
        """Fixture for a sample UUID."""
        return uuid4()
    
    def test_buoy_gear_creation_minimal(self, sample_gear_id, sample_datetime, sample_devices):
        """Test BuoyGear creation with minimal required fields."""
        gear = BuoyGear(
            id=sample_gear_id,
            display_id="GEAR_001",
            name="Test Gear",
            status="active",
            last_updated=sample_datetime,
            devices=sample_devices,
            type="fishing_gear",
            manufacturer="Test Manufacturer"
        )
        
        assert gear.id == sample_gear_id
        assert gear.display_id == "GEAR_001"
        assert gear.name == "Test Gear"
        assert gear.status == "active"
        assert gear.last_updated == sample_datetime
        assert gear.devices == sample_devices
        assert gear.type == "fishing_gear"
        assert gear.manufacturer == "Test Manufacturer"
        assert gear.location is None
        assert gear.additional is None
    
    def test_buoy_gear_creation_full(self, sample_gear_id, sample_datetime, sample_devices, sample_location):
        """Test BuoyGear creation with all fields."""
        additional_data = {"custom_field": "custom_value", "number": 42}
        
        gear = BuoyGear(
            id=sample_gear_id,
            display_id="GEAR_002",
            name="Full Test Gear",
            status="deployed",
            last_updated=sample_datetime,
            devices=sample_devices,
            type="lobster_trap",
            manufacturer="Full Manufacturer",
            location=sample_location,
            additional=additional_data
        )
        
        assert gear.id == sample_gear_id
        assert gear.display_id == "GEAR_002"
        assert gear.name == "Full Test Gear"
        assert gear.status == "deployed"
        assert gear.last_updated == sample_datetime
        assert gear.devices == sample_devices
        assert gear.type == "lobster_trap"
        assert gear.manufacturer == "Full Manufacturer"
        assert gear.location == sample_location
        assert gear.additional == additional_data
    
    def test_buoy_gear_empty_devices(self, sample_gear_id, sample_datetime):
        """Test BuoyGear with empty devices list."""
        gear = BuoyGear(
            id=sample_gear_id,
            display_id="GEAR_003",
            name="Empty Gear",
            status="inactive",
            last_updated=sample_datetime,
            devices=[],
            type="test_gear",
            manufacturer="Test Manufacturer"
        )
        
        assert gear.devices == []
    
    def test_buoy_gear_validation_errors(self):
        """Test BuoyGear validation errors."""
        # Missing required fields
        with pytest.raises(ValidationError):
            BuoyGear()
        
        # Invalid UUID
        with pytest.raises(ValidationError):
            BuoyGear(
                id="invalid_uuid",
                display_id="GEAR_001",
                name="Test Gear",
                status="active",
                last_updated=datetime.now(),
                devices=[],
                type="fishing_gear",
                manufacturer="Test Manufacturer"
            )
        
        # Invalid datetime
        with pytest.raises(ValidationError):
            BuoyGear(
                id=uuid4(),
                display_id="GEAR_001",
                name="Test Gear",
                status="active",
                last_updated="invalid_date",
                devices=[],
                type="fishing_gear",
                manufacturer="Test Manufacturer"
            )
    
    def test_create_haul_observation_single_device(self, sample_gear_id, sample_datetime, sample_location):
        """Test create_haul_observation with single device."""
        device = BuoyDevice(
            device_id="device_001",
            label="Single Device",
            location=sample_location,
            last_updated=sample_datetime,
            last_deployed=sample_datetime
        )
        
        gear = BuoyGear(
            id=sample_gear_id,
            display_id="GEAR_SINGLE",
            name="Single Device Gear",
            status="active",
            last_updated=sample_datetime,
            devices=[device],
            type="fishing_gear",
            manufacturer="Test Manufacturer"
        )
        
        recorded_at = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
        observations = gear.create_haul_observation(recorded_at)
        
        assert len(observations) == 1
        observation = observations[0]
        
        assert observation["subject_name"] == "GEAR_SINGLE"
        assert observation["manufacturer_id"] == "device_001"
        assert observation["subject_is_active"] is False
        assert observation["source_type"] == "ropeless_buoy"
        assert observation["subject_subtype"] == "ropeless_buoy_gearset"
        assert observation["location"]["lat"] == 42.123456
        assert observation["location"]["lon"] == -71.987654
        assert observation["recorded_at"] == recorded_at
    
    def test_create_haul_observation_multiple_devices(self, sample_gear_id, sample_datetime, sample_devices):
        """Test create_haul_observation with multiple devices."""
        gear = BuoyGear(
            id=sample_gear_id,
            display_id="GEAR_MULTI",
            name="Multi Device Gear",
            status="active",
            last_updated=sample_datetime,
            devices=sample_devices,
            type="fishing_gear",
            manufacturer="Test Manufacturer"
        )
        
        recorded_at = datetime(2023, 10, 1, 15, 30, 0, tzinfo=timezone.utc)
        observations = gear.create_haul_observation(recorded_at)
        
        assert len(observations) == 2
        
        # Check first observation
        obs1 = observations[0]
        assert obs1["subject_name"] == "GEAR_MULTI"
        assert obs1["manufacturer_id"] == "device_001"
        assert obs1["subject_is_active"] is False
        assert obs1["source_type"] == "ropeless_buoy"
        assert obs1["subject_subtype"] == "ropeless_buoy_gearset"
        assert obs1["location"]["lat"] == 42.123456
        assert obs1["location"]["lon"] == -71.987654
        assert obs1["recorded_at"] == recorded_at
        
        # Check second observation
        obs2 = observations[1]
        assert obs2["subject_name"] == "GEAR_MULTI"
        assert obs2["manufacturer_id"] == "device_002"
        assert obs2["subject_is_active"] is False
        assert obs2["source_type"] == "ropeless_buoy"
        assert obs2["subject_subtype"] == "ropeless_buoy_gearset"
        assert obs2["location"]["lat"] == 43.0
        assert obs2["location"]["lon"] == -72.0
        assert obs2["recorded_at"] == recorded_at
    
    def test_create_haul_observation_empty_devices(self, sample_gear_id, sample_datetime):
        """Test create_haul_observation with no devices."""
        gear = BuoyGear(
            id=sample_gear_id,
            display_id="GEAR_EMPTY",
            name="Empty Gear",
            status="active",
            last_updated=sample_datetime,
            devices=[],
            type="fishing_gear",
            manufacturer="Test Manufacturer"
        )
        
        recorded_at = datetime(2023, 10, 1, 9, 0, 0, tzinfo=timezone.utc)
        observations = gear.create_haul_observation(recorded_at)
        
        assert len(observations) == 0
        assert observations == []
    
    def test_create_haul_observation_imports_correctly(self, sample_gear_id, sample_datetime, sample_devices):
        """Test that create_haul_observation correctly imports constants."""
        gear = BuoyGear(
            id=sample_gear_id,
            display_id="GEAR_IMPORT",
            name="Import Test Gear",
            status="active",
            last_updated=sample_datetime,
            devices=sample_devices,
            type="fishing_gear",
            manufacturer="Test Manufacturer"
        )
        
        recorded_at = datetime.now(timezone.utc)
        observations = gear.create_haul_observation(recorded_at)
        
        # Verify that the constants are imported and used correctly
        for observation in observations:
            assert observation["source_type"] == "ropeless_buoy"
            assert observation["subject_subtype"] == "ropeless_buoy_gearset"