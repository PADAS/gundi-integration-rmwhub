import pytest
from datetime import datetime, timezone

from app.actions.tests.factories import TrapFactory, GearsetFactory
from app.actions.rmwhub.types import GearSet, Trap


@pytest.mark.asyncio
async def test_gearset_validators():
    """
    Test GearSet validators for none_to_empty and none_to_empty_list
    """
    # Test none_to_empty validator for trawl_path (line 85)
    gearset_data = {
        "vessel_id": "vessel_001",
        "id": "set_001",
        "deployment_type": "trawl",
        "traps_in_set": 2,
        "trawl_path": None,  # This should be converted to empty string
        "share_with": None,  # This should be converted to empty list
        "traps": [],
        "when_updated_utc": datetime.now(timezone.utc).isoformat(),
    }
    
    gearset = GearSet(**gearset_data)
    
    # Test that None trawl_path becomes empty string (line 87)
    assert gearset.trawl_path == ""
    
    # Test that None share_with becomes empty list (line 91)
    assert gearset.share_with == []


@pytest.mark.asyncio
async def test_gearset_dict_access():
    """
    Test GearSet __getitem__ and get methods
    """
    gearset = GearsetFactory.create(
        traps_in_set=1,
        vessel_id="vessel_123",
        set_id="set_456"
    )
    
    # Test __getitem__ method (line 95)
    assert gearset["vessel_id"] == "vessel_123"
    assert gearset["id"] == "set_456"
    
    # Test get method (line 98)
    assert gearset.get("vessel_id") == "vessel_123"
    assert gearset.get("id") == "set_456"


@pytest.mark.asyncio
async def test_gearset_hash():
    """
    Test GearSet __hash__ method
    """
    trap1 = TrapFactory.create(trap_id="trap_1")
    trap2 = TrapFactory.create(trap_id="trap_2")
    
    gearset1 = GearSet(
        vessel_id="vessel_001",
        id="set_001",
        deployment_type="trawl",
        traps_in_set=2,
        trawl_path="path",
        share_with=[],
        traps=[trap1, trap2],
        when_updated_utc=datetime.now(timezone.utc).isoformat(),
    )
    
    gearset2 = GearSet(
        vessel_id="vessel_001",
        id="set_001",
        deployment_type="trawl",
        traps_in_set=2,
        trawl_path="path",
        share_with=[],
        traps=[trap1, trap2],
        when_updated_utc=datetime.now(timezone.utc).isoformat(),
    )
    
    # Test __hash__ method (line 101)
    assert hash(gearset1) == hash(gearset2)


@pytest.mark.asyncio
async def test_gearset_create_observations_empty_traps():
    """
    Test GearSet.create_observations with empty traps list
    """
    gearset = GearSet(
        vessel_id="vessel_001",
        id="set_001",
        deployment_type="trawl",
        traps_in_set=0,
        trawl_path="path",
        share_with=[],
        traps=[],  # Empty traps list
        when_updated_utc=datetime.now(timezone.utc).isoformat(),
    )
    
    # Test empty traps case (line 129)
    observations = await gearset.create_observations()
    assert observations == []


@pytest.mark.asyncio
async def test_gearset_create_observations_with_traps():
    """
    Test GearSet.create_observations with traps
    """
    trap1 = TrapFactory.create(
        trap_id="trap_1",
        sequence=1,
        latitude=40.0,
        longitude=-70.0,
        status="deployed"
    )
    trap2 = TrapFactory.create(
        trap_id="trap_2", 
        sequence=2,
        latitude=41.0,
        longitude=-71.0,
        status="retrieved"
    )
    
    gearset = GearSet(
        vessel_id="vessel_001",
        id="set_001",
        deployment_type="trawl",
        traps_in_set=2,
        trawl_path="path",
        share_with=[],
        traps=[trap1, trap2],
        when_updated_utc="2023-01-01T12:00:00Z",
    )
    
    observations = await gearset.create_observations()
    
    assert len(observations) == 1
    observation = observations[0]
    
    # Test observation structure
    assert "rmwhub_" in observation["name"]
    assert observation["type"] == "ropeless_buoy"
    assert observation["subject_type"] == "ropeless_buoy_gearset"
    assert observation["is_active"] is True  # Has deployed trap
    assert observation["location"]["lat"] == 40.0  # Primary trap location
    assert observation["location"]["lon"] == -70.0
    assert observation["additional"]["rmwhub_set_id"] == "set_001"
    assert observation["additional"]["deployment_type"] == "trawl"
    assert observation["additional"]["traps_in_set"] == 2
    assert observation["additional"]["vessel_id"] == "vessel_001"
    assert len(observation["additional"]["devices"]) == 2


@pytest.mark.asyncio
async def test_gearset_get_trap_ids():
    """
    Test GearSet.get_trap_ids method
    """
    trap1 = TrapFactory.create(trap_id="e_trap_1")
    trap2 = TrapFactory.create(trap_id="rmwhub_trap_2")
    trap3 = TrapFactory.create(trap_id="regular_trap_3")
    
    gearset = GearSet(
        vessel_id="vessel_001",
        id="set_001",
        deployment_type="trawl",
        traps_in_set=3,
        trawl_path="path",
        share_with=[],
        traps=[trap1, trap2, trap3],
        when_updated_utc=datetime.now(timezone.utc).isoformat(),
    )
    
    # Test get_trap_ids method (line 168)
    trap_ids = await gearset.get_trap_ids()
    
    # Should remove "e_" and "rmwhub_" prefixes
    expected_ids = {"trap_1", "trap_2", "regular_trap_3"}
    assert trap_ids == expected_ids


@pytest.mark.asyncio
async def test_gearset_is_visited():
    """
    Test GearSet.is_visited method
    """
    trap1 = TrapFactory.create(trap_id="trap_1")
    trap2 = TrapFactory.create(trap_id="trap_2")
    
    gearset = GearSet(
        vessel_id="vessel_001",
        id="set_001",
        deployment_type="trawl",
        traps_in_set=2,
        trawl_path="path",
        share_with=[],
        traps=[trap1, trap2],
        when_updated_utc=datetime.now(timezone.utc).isoformat(),
    )
    
    # Test is_visited method (line 177)
    visited_set = {"trap_1", "other_trap"}
    is_visited = await gearset.is_visited(visited_set)
    assert bool(is_visited) is True  # trap_1 is in visited set
    
    # Test with no intersection
    visited_set_no_match = {"other_trap", "another_trap"}
    is_not_visited = await gearset.is_visited(visited_set_no_match)
    assert bool(is_not_visited) is False  # No traps in visited set
