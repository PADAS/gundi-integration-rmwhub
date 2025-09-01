import pytest
from datetime import datetime, timezone
from uuid import uuid4
from unittest.mock import Mock

from app.actions.buoy.types import (
    BuoyGear, 
    BuoyDevice, 
    DeviceLocation, 
    ObservationSubject,
    Feature,
    Geometry,
    FeatureProperties,
    CoordinateProperties,
    LastPositionStatus
)


@pytest.mark.asyncio
async def test_buoy_gear_create_haul_observation():
    """
    Test BuoyGear.create_haul_observation method (lines 38-40)
    """
    # Create a BuoyGear with devices
    device_location = DeviceLocation(latitude=40.0, longitude=-70.0)
    device = BuoyDevice(
        device_id="test_device_001",
        label="a",
        location=device_location,
        last_updated=datetime.now(timezone.utc),
        last_deployed=datetime.now(timezone.utc)
    )
    
    buoy_gear = BuoyGear(
        id=uuid4(),
        display_id="test_display_id",
        name="Test Gear",
        status="deployed",
        last_updated=datetime.now(timezone.utc),
        devices=[device],
        type="ropeless_buoy",
        manufacturer="test_manufacturer"
    )
    
    recorded_at = datetime.now(timezone.utc)
    
    # Test create_haul_observation method
    observations = buoy_gear.create_haul_observation(recorded_at)
    
    assert len(observations) == 1
    observation = observations[0]
    
    assert observation["subject_name"] == "test_display_id"
    assert observation["manufacturer_id"] == "test_device_001"
    assert observation["subject_is_active"] is False
    assert observation["location"]["lat"] == 40.0
    assert observation["location"]["lon"] == -70.0
    assert observation["recorded_at"] == recorded_at


@pytest.mark.asyncio
async def test_observation_subject_location_property():
    """
    Test ObservationSubject.location property (line 120)
    """
    # Create mock coordinate properties
    coord_props = CoordinateProperties(time=datetime.now(timezone.utc))
    
    # Create mock geometry
    geometry = Geometry(
        type="Point",
        coordinates=[-70.0, 40.0]  # [longitude, latitude]
    )
    
    # Create mock feature properties - using dict with aliases
    feature_props_data = {
        "title": "Test Subject",
        "subject_type": "ropeless_buoy",
        "subject_subtype": "ropeless_buoy_gearset",
        "id": uuid4(),
        "stroke": "#FFFF00",
        "stroke-opacity": 1.0,  # Using alias with hyphen
        "stroke-width": 2,      # Using alias with hyphen
        "image": "test_image.png",
        "last_voice_call_start_at": None,
        "location_requested_at": None,
        "radio_state_at": datetime.now(timezone.utc),
        "radio_state": "na",
        "coordinateProperties": coord_props,
        "DateTime": datetime.now(timezone.utc)
    }
    feature_props = FeatureProperties(**feature_props_data)
    
    # Create mock feature
    feature = Feature(
        type="Feature",
        geometry=geometry,
        properties=feature_props
    )
    
    # Create ObservationSubject
    subject = ObservationSubject(
        content_type="observations.subject",
        id=uuid4(),
        name="Test Subject",
        subject_type="ropeless_buoy",
        subject_subtype="ropeless_buoy_gearset",
        common_name=None,
        additional={"devices": []},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        is_active=True,
        user=None,
        tracks_available=False,
        image_url="/static/pin-black.svg",
        last_position_status=None,
        last_position_date=datetime.now(timezone.utc),
        last_position=feature,
        device_status_properties=None,
        url="https://example.com/subject/test"
    )
    
    # Test location property (line 120)
    location = subject.location
    assert location == (40.0, -70.0)  # (latitude, longitude)


@pytest.mark.asyncio
async def test_observation_subject_latitude_error():
    """
    Test ObservationSubject.latitude property with no position (lines 127-129)
    """
    subject = ObservationSubject(
        content_type="observations.subject",
        id=uuid4(),
        name="Test Subject",
        subject_type="ropeless_buoy",
        subject_subtype="ropeless_buoy_gearset",
        common_name=None,
        additional={"devices": []},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        is_active=True,
        user=None,
        tracks_available=False,
        image_url="/static/pin-black.svg",
        last_position_status=None,
        last_position_date=None,
        last_position=None,  # No position
        device_status_properties=None,
        url="https://example.com/subject/test"
    )
    
    # Test latitude property error (lines 127-129)
    with pytest.raises(ValueError, match="Last position is not available"):
        _ = subject.latitude


@pytest.mark.asyncio
async def test_observation_subject_longitude_error():
    """
    Test ObservationSubject.longitude property with no position (lines 136-138)
    """
    subject = ObservationSubject(
        content_type="observations.subject",
        id=uuid4(),
        name="Test Subject",
        subject_type="ropeless_buoy",
        subject_subtype="ropeless_buoy_gearset",
        common_name=None,
        additional={"devices": []},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        is_active=True,
        user=None,
        tracks_available=False,
        image_url="/static/pin-black.svg",
        last_position_status=None,
        last_position_date=None,
        last_position=None,  # No position
        device_status_properties=None,
        url="https://example.com/subject/test"
    )
    
    # Test longitude property error (lines 136-138)
    with pytest.raises(ValueError, match="Last position is not available"):
        _ = subject.longitude


@pytest.mark.asyncio
async def test_observation_subject_create_observation_no_position():
    """
    Test ObservationSubject.create_observation with no position (line 150)
    """
    subject = ObservationSubject(
        content_type="observations.subject",
        id=uuid4(),
        name="Test Subject",
        subject_type="ropeless_buoy",
        subject_subtype="ropeless_buoy_gearset",
        common_name=None,
        additional={"devices": [{"device_id": "test"}]},
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        is_active=True,
        user=None,
        tracks_available=False,
        image_url="/static/pin-black.svg",
        last_position_status=None,
        last_position_date=None,
        last_position=None,  # No position
        device_status_properties=None,
        url="https://example.com/subject/test"
    )
    
    # Test create_observation error with no position (line 150)
    with pytest.raises(ValueError, match="Last position is not available"):
        subject.create_observation(datetime.now(timezone.utc))


@pytest.mark.asyncio
async def test_observation_subject_create_observation_no_devices():
    """
    Test ObservationSubject.create_observation with no devices (line 153)
    """
    # Create mock geometry and feature
    geometry = Geometry(type="Point", coordinates=[-70.0, 40.0])
    coord_props = CoordinateProperties(time=datetime.now(timezone.utc))
    feature_props_data = {
        "title": "Test Subject",
        "subject_type": "ropeless_buoy",
        "subject_subtype": "ropeless_buoy_gearset",
        "id": uuid4(),
        "stroke": "#FFFF00",
        "stroke-opacity": 1.0,
        "stroke-width": 2,
        "image": "test_image.png",
        "last_voice_call_start_at": None,
        "location_requested_at": None,
        "radio_state_at": datetime.now(timezone.utc),
        "radio_state": "na",
        "coordinateProperties": coord_props,
        "DateTime": datetime.now(timezone.utc)
    }
    feature_props = FeatureProperties(**feature_props_data)
    feature = Feature(type="Feature", geometry=geometry, properties=feature_props)
    
    subject = ObservationSubject(
        content_type="observations.subject",
        id=uuid4(),
        name="Test Subject",
        subject_type="ropeless_buoy",
        subject_subtype="ropeless_buoy_gearset",
        common_name=None,
        additional={},  # No devices
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        is_active=True,
        user=None,
        tracks_available=False,
        image_url="/static/pin-black.svg",
        last_position_status=None,
        last_position_date=datetime.now(timezone.utc),
        last_position=feature,
        device_status_properties=None,
        url="https://example.com/subject/test"
    )
    
    # Test create_observation error with no devices (line 153)
    with pytest.raises(ValueError, match="No devices available in additional information"):
        subject.create_observation(datetime.now(timezone.utc))


@pytest.mark.asyncio
async def test_observation_subject_create_observation_success():
    """
    Test ObservationSubject.create_observation successful case (lines 155-177)
    """
    # Create mock geometry and feature
    geometry = Geometry(type="Point", coordinates=[-70.0, 40.0])
    coord_props = CoordinateProperties(time=datetime.now(timezone.utc))
    feature_props_data = {
        "title": "Test Subject",
        "subject_type": "ropeless_buoy",
        "subject_subtype": "ropeless_buoy_gearset",
        "id": uuid4(),
        "stroke": "#FFFF00",
        "stroke-opacity": 1.0,
        "stroke-width": 2,
        "image": "test_image.png",
        "last_voice_call_start_at": None,
        "location_requested_at": None,
        "radio_state_at": datetime.now(timezone.utc),
        "radio_state": "na",
        "coordinateProperties": coord_props,
        "DateTime": datetime.now(timezone.utc)
    }
    feature_props = FeatureProperties(**feature_props_data)
    feature = Feature(type="Feature", geometry=geometry, properties=feature_props)
    
    subject = ObservationSubject(
        content_type="observations.subject",
        id=uuid4(),
        name="Test Subject",
        subject_type="ropeless_buoy",
        subject_subtype="ropeless_buoy_gearset",
        common_name=None,
        additional={
            "devices": [{"device_id": "test_device"}],
            "rmwhub_set_id": "test_set_001",
            "display_id": "test_display"
        },
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        is_active=True,
        user=None,
        tracks_available=False,
        image_url="/static/pin-black.svg",
        last_position_status=None,
        last_position_date=datetime.now(timezone.utc),
        last_position=feature,
        device_status_properties=None,
        url="https://example.com/subject/test"
    )
    
    recorded_at = datetime.now(timezone.utc)
    
    # Test create_observation successful case (lines 155-177)
    observation = subject.create_observation(recorded_at)
    
    assert observation["name"] == "Test Subject"
    assert observation["source"] == "Test Subject"
    assert observation["type"] == "ropeless_buoy"
    assert observation["subject_type"] == "ropeless_buoy_gearset"
    assert observation["location"]["lat"] == 40.0
    assert observation["location"]["lon"] == -70.0
    assert observation["additional"]["subject_name"] == "Test Subject"
    assert observation["additional"]["rmwhub_set_id"] == "test_set_001"
    assert observation["additional"]["display_id"] == "test_display"
    assert observation["additional"]["subject_is_active"] is True
    assert len(observation["additional"]["devices"]) == 1


@pytest.mark.asyncio
async def test_observation_subject_create_observation_with_is_active_override():
    """
    Test ObservationSubject.create_observation with is_active override
    """
    # Create mock geometry and feature
    geometry = Geometry(type="Point", coordinates=[-70.0, 40.0])
    coord_props = CoordinateProperties(time=datetime.now(timezone.utc))
    feature_props_data = {
        "title": "Test Subject",
        "subject_type": "ropeless_buoy",
        "subject_subtype": "ropeless_buoy_gearset",
        "id": uuid4(),
        "stroke": "#FFFF00",
        "stroke-opacity": 1.0,
        "stroke-width": 2,
        "image": "test_image.png",
        "last_voice_call_start_at": None,
        "location_requested_at": None,
        "radio_state_at": datetime.now(timezone.utc),
        "radio_state": "na",
        "coordinateProperties": coord_props,
        "DateTime": datetime.now(timezone.utc)
    }
    feature_props = FeatureProperties(**feature_props_data)
    feature = Feature(type="Feature", geometry=geometry, properties=feature_props)
    
    subject = ObservationSubject(
        content_type="observations.subject",
        id=uuid4(),
        name="Test Subject",
        subject_type="ropeless_buoy",
        subject_subtype="ropeless_buoy_gearset",
        common_name=None,
        additional={
            "devices": [{"device_id": "test_device"}],
            "rmwhub_set_id": "test_set_001",
            "display_id": "test_display"
        },
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        is_active=True,  # Subject is active
        user=None,
        tracks_available=False,
        image_url="/static/pin-black.svg",
        last_position_status=None,
        last_position_date=datetime.now(timezone.utc),
        last_position=feature,
        device_status_properties=None,
        url="https://example.com/subject/test"
    )
    
    recorded_at = datetime.now(timezone.utc)
    
    # Test with is_active override to False
    observation = subject.create_observation(recorded_at, is_active=False)
    
    assert observation["additional"]["subject_is_active"] is False
    assert "gear_retrieved" in observation["additional"]["event_type"]
