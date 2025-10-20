# RMW Hub Integration Documentation

## Overview

This document describes the bidirectional integration between RMW Hub (Ropeless Monitoring and Warning Hub) and our buoy tracking system (Earth Ranger destinations). It focuses on:

1. **RMW Hub API Connection** - Authentication and data synchronization mechanisms
2. **Data Structure** - Expected format from RMW Hub and our system
3. **Record Filtering** - Which gear records we process vs. discard
4. **Observation Mapping** - How RMW Hub data transforms into our observation format
5. **Upload Process** - How we share gear data back to RMW Hub

The integration runs on two schedules:
- **Every 3 minutes**: Download from RMW Hub + Upload to RMW Hub (90-day sync window)
- **Daily at 12:10 AM**: Full sync with configurable time window

---

## RMW Hub API Connection

### Authentication

**API Key-Based Authentication**
- **Method**: API key sent in request body
- **No token refresh**: Single persistent API key
- **Configuration**:
  ```python
  PullRmwHubObservationsConfiguration:
    - api_key: Secret API key for RMW Hub
    - rmw_url: Base URL (default: "https://test.ropeless.network/api/")
    - share_with: List of entities to share data with
    - minutes_to_sync: Historical data window (default: 30 minutes)
  ```

### Data Retrieval Process (Download)

RMW Hub provides gear data through a **search_hub** endpoint:

#### Search Hub Request
```python
POST {rmw_url}/search_hub/

Headers:
  accept: application/json
  Content-Type: application/json

Body:
{
  "format_version": 0.1,
  "api_key": "<api_key>",
  "max_sets": 10000,
  "start_datetime_utc": "2025-10-20T00:00:00+00:00"
}
```

**Parameters**:
- `format_version`: API format version (currently 0.1)
- `api_key`: Authentication key
- `max_sets`: Maximum number of gear sets to return
- `start_datetime_utc`: Filter sets updated after this timestamp

#### Response Format
```json
{
  "sets": [
    {
      "vessel_id": "vessel_123",
      "set_id": "set_456",
      "deployment_type": "trawl",
      "traps_in_set": 5,
      "trawl_path": {},
      "share_with": ["entity1", "entity2"],
      "when_updated_utc": "2025-10-20T10:00:00+00:00",
      "traps": [
        {
          "trap_id": "trap_789",
          "sequence": 1,
          "latitude": 42.123456,
          "longitude": -70.654321,
          "deploy_datetime_utc": "2025-10-20T08:00:00+00:00",
          "surface_datetime_utc": null,
          "retrieved_datetime_utc": null,
          "status": "deployed",
          "accuracy": "gps",
          "release_type": "",
          "is_on_end": false
        }
      ]
    }
  ]
}
```

### Data Upload Process

We share gear data back to RMW Hub through an **upload_deployments** endpoint:

#### Upload Deployments Request
```python
POST {rmw_url}/upload_deployments/

Headers:
  accept: application/json
  Content-Type: application/json

Body:
{
  "format_version": 0,
  "api_key": "<api_key>",
  "sets": [
    {
      "set_id": "uuid-here",
      "vessel_id": "",
      "deployment_type": "trawl",
      "when_updated_utc": "2025-10-20T10:00:00+00:00",
      "traps": [
        {
          "trap_id": "device_source_id",
          "sequence": 1,
          "latitude": 42.123456,
          "longitude": -70.654321,
          "deploy_datetime_utc": "2025-10-20T08:00:00+00:00",
          "retrieved_datetime_utc": null,
          "status": "deployed",
          "accuracy": "gps",
          "is_on_end": false,
          "manufacturer": "edgetech",
          "serial_number": "ET-12345"
        }
      ]
    }
  ]
}
```

**Field Transformations for Upload**:
- `id` → `set_id` (at gear set level)
- `id` → `trap_id` (at trap level)
- `release_type`: Set to empty string if null

#### Upload Response
```json
{
  "result": {
    "trap_count": 10,
    "failed_sets": []
  }
}
```

---

## RMW Hub Data Structure

### GearSet Object

Represents a deployment of fishing gear (one or more traps).

```python
class GearSet(BaseModel):
    vessel_id: str                    # Vessel identifier
    id: str                           # Unique gear set ID
    deployment_type: str              # "trawl" or "single"
    traps_in_set: Optional[int]       # Number of traps in set
    trawl_path: Optional[dict]        # Trawl path geometry
    share_with: Optional[List[str]]   # Entities to share with
    traps: List[Trap]                 # List of trap objects
    when_updated_utc: str             # Last update timestamp
```

**Deployment Types**:
- `"trawl"`: Multiple traps (>1)
- `"single"`: Single trap deployment

### Trap Object

Represents an individual buoy/trap within a gear set.

```python
class Trap(BaseModel):
    id: str                                  # Unique trap ID
    sequence: int                            # Position in gear set (1-based)
    latitude: float                          # Deployment latitude
    longitude: float                         # Deployment longitude
    manufacturer: Optional[str]              # Manufacturer name
    serial_number: Optional[str]             # Device serial number
    deploy_datetime_utc: Optional[str]       # Deployment timestamp
    surface_datetime_utc: Optional[str]      # Surface timestamp
    retrieved_datetime_utc: Optional[str]    # Retrieval timestamp
    status: str                              # "deployed" or "retrieved"
    accuracy: str                            # Location accuracy ("gps", etc.)
    release_type: Optional[str]              # Release mechanism type
    is_on_end: bool                          # Whether this is the last trap
```

**Key Fields**:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique trap identifier (becomes `source` in observations) |
| `sequence` | int | Position in the gear set (1 = first, N = last) |
| `status` | string | "deployed" or "retrieved" |
| `deploy_datetime_utc` | string? | When trap was deployed (ISO 8601) |
| `retrieved_datetime_utc` | string? | When trap was retrieved (ISO 8601) |
| `is_on_end` | boolean | True for the last trap in a multi-trap set |

### Timestamp Priority

The `get_latest_update_time()` method determines the trap's recorded time:

**For Deployed Status**:
```python
recorded_at = deploy_datetime_utc or now()
```

**For Retrieved Status**:
```python
recorded_at = retrieved_datetime_utc or surface_datetime_utc or deploy_datetime_utc or now()
```

**Priority**: `retrieved` > `surfaced` > `deployed` > `current time`

---

## Record Filtering

### Download Filtering (RMW Hub → Our System)

#### Records We Process

✅ **Included Records**
- All gear sets returned by RMW Hub for the time window
- Traps with valid status ("deployed" or "retrieved")
- Traps with valid location data (latitude/longitude)

✅ **Status-Based Processing**

| RMW Hub Status | Earth Ranger Status | Action |
|----------------|---------------------|--------|
| `deployed` | Not exists | Create deployment observation |
| `deployed` | `deployed` | Skip (already deployed) |
| `deployed` | `retrieved` | Create deployment observation (re-deployment) |
| `retrieved` | Not exists | Skip (warn - no history) |
| `retrieved` | `deployed` | Create retrieval observation |
| `retrieved` | `retrieved` | Skip (already retrieved) |

#### Records We Discard

❌ **Excluded During Processing**

| Condition | Reason | Log Level |
|-----------|--------|-----------|
| Retrieved trap not in ER | No deployment history | INFO |
| Same status in both systems | Already synchronized | INFO |
| Invalid trap status | Unknown state | ERROR |

**Filter Implementation**:
```python
async def process_download(self, rmw_sets: List[GearSet]) -> List:
    gears = await self.gear_client.get_all_gears()
    
    # Create mapping of trap_id to ER gear
    trap_id_to_gear_mapping = {
        device.device_id: gear
        for gear in gears
        for device in gear.devices
        if device.source_id
    }
    
    observations = []
    for gearset in rmw_sets:
        for trap in gearset.traps:
            er_gear = trap_id_to_gear_mapping.get(trap.id)
            
            # Skip retrieved traps with no ER history
            if not er_gear and trap.status == "retrieved":
                continue
            
            # Skip if status matches
            if er_gear and trap.status == er_gear.status:
                continue
            
            # Create observation for status change
            observation = await gearset.build_observation_for_specific_trap(trap.id)
            observations.extend(observation)
    
    return observations
```

### Upload Filtering (Our System → RMW Hub)

#### Records We Upload

✅ **Included Records**
- Gears from Earth Ranger updated after `start_datetime`
- Both deployed and retrieved gears
- All manufacturers **except** `"rmwhub"` (avoid circular uploads)

✅ **Streaming Approach**
- Uses async generator to process gears one-by-one
- Separate iterations for `state="hauled"` and `state="deployed"`
- Memory-efficient for large datasets

#### Records We Exclude

❌ **Excluded from Upload**

| Condition | Reason |
|-----------|--------|
| `manufacturer == "rmwhub"` | Avoid uploading RMW Hub's own data back |
| Gears without devices | Invalid gear structure |
| Processing errors | Individual gear failures don't stop batch |

**Upload Implementation**:
```python
async def process_upload(self, start_datetime: datetime) -> Tuple[int, dict]:
    rmw_updates = []
    
    # Stream hauled gears
    async for er_gear in self.iter_er_gears(start_datetime=start_datetime, state="hauled"):
        if er_gear.manufacturer == "rmwhub":
            continue  # Skip RMW Hub gears
        
        rmw_update = await self._create_rmw_update_from_er_gear(er_gear)
        if rmw_update:
            rmw_updates.append(rmw_update)
    
    # Stream deployed gears
    async for er_gear in self.iter_er_gears(start_datetime=start_datetime, state="deployed"):
        rmw_update = await self._create_rmw_update_from_er_gear(er_gear)
        if rmw_update:
            rmw_updates.append(rmw_update)
    
    # Upload to RMW Hub
    response = await self.rmw_client.upload_data(rmw_updates)
    return trap_count, response_data
```

---

## Observation Mapping

### Our Observation Schema

We transform RMW Hub trap data into standardized observation records:

```json
{
  "source_name": "<gear_set_id>",
  "source": "<trap_id>",
  "location": {
    "lat": <latitude>,
    "lon": <longitude>
  },
  "recorded_at": "<iso8601_timestamp>",
  "type": "ropeless_buoy",
  "subject_type": "ropeless_buoy_gearset",
  "additional": {
    "event_type": "trap_deployed" | "trap_retrieved",
    "raw": { /* Complete GearSet object */ },
    "example_field": "example_value"
  }
}
```

### Field Mapping (RMW Hub → Our System)

| Our Field | RMW Hub Source | Notes |
|-----------|---------------|-------|
| `source_name` | `GearSet.id` | Gear set identifier |
| `source` | `Trap.id` | Individual trap identifier |
| `type` | Static | Always `"ropeless_buoy"` |
| `subject_type` | Static | Always `"ropeless_buoy_gearset"` |
| `recorded_at` | Derived | See timestamp priority logic |
| `location.lat` | `Trap.latitude` | Deployment latitude |
| `location.lon` | `Trap.longitude` | Deployment longitude |
| `event_type` | `Trap.status` | "deployed" → `trap_deployed`, "retrieved" → `trap_retrieved` |
| `raw` | `GearSet.dict()` | Complete gear set data |

### Event Types

#### trap_deployed

Created when:
- New trap appears in RMW Hub with status "deployed"
- Trap changes from "retrieved" to "deployed" (re-deployment)

**Timestamp**: `deploy_datetime_utc`

```json
{
  "additional": {
    "event_type": "trap_deployed",
    "raw": { /* GearSet data */ }
  }
}
```

#### trap_retrieved

Created when:
- Trap changes from "deployed" to "retrieved"

**Timestamp Priority**: `retrieved_datetime_utc` > `surface_datetime_utc` > `deploy_datetime_utc`

```json
{
  "additional": {
    "event_type": "trap_retrieved",
    "raw": { /* GearSet data */ }
  }
}
```

---

## Reverse Mapping (Our System → RMW Hub)

### Earth Ranger Gear to RMW Hub GearSet

We transform Earth Ranger gears back into RMW Hub format for upload:

```python
async def _create_rmw_update_from_er_gear(self, er_gear: BuoyGear) -> GearSet:
    traps = []
    for i, device in enumerate(er_gear.devices):
        traps.append(Trap(
            id=device.source_id,
            sequence=i + 1,
            latitude=device.location.latitude,
            longitude=device.location.longitude,
            deploy_datetime_utc=device.last_deployed.isoformat(),
            surface_datetime_utc=None,
            accuracy="gps",
            retrieved_datetime_utc=device.last_updated.isoformat() if er_gear.status == "retrieved" else None,
            status="deployed" if er_gear.status == "deployed" else "retrieved",
            is_on_end=i == len(er_gear.devices) - 1,
            manufacturer=er_gear.manufacturer,
            serial_number=self._get_serial_number_from_device_id(device.device_id, er_gear.manufacturer)
        ))
    
    return GearSet(
        vessel_id="",
        id=str(er_gear.id),
        deployment_type="trawl" if len(er_gear.devices) > 1 else "single",
        traps=traps,
        when_updated_utc=er_gear.last_updated.isoformat()
    )
```

### Field Mapping (Our System → RMW Hub)

| RMW Hub Field | Our Source | Notes |
|---------------|-----------|-------|
| `set_id` | `BuoyGear.id` | UUID converted to string |
| `vessel_id` | Empty | Not tracked in our system |
| `deployment_type` | Derived | "trawl" if >1 device, else "single" |
| `when_updated_utc` | `BuoyGear.last_updated` | ISO 8601 format |
| `trap_id` | `BuoyDevice.source_id` | Device source identifier |
| `sequence` | Index + 1 | 1-based position |
| `latitude` | `BuoyDevice.location.latitude` | Device latitude |
| `longitude` | `BuoyDevice.location.longitude` | Device longitude |
| `deploy_datetime_utc` | `BuoyDevice.last_deployed` | Deployment timestamp |
| `retrieved_datetime_utc` | `BuoyDevice.last_updated` | Only if status is "retrieved" |
| `status` | `BuoyGear.status` | "deployed" or "retrieved" |
| `accuracy` | Static | Always "gps" |
| `is_on_end` | Derived | True for last device |
| `manufacturer` | `BuoyGear.manufacturer` | Manufacturer name |
| `serial_number` | Derived | Extracted from device_id |

### Serial Number Extraction

Different manufacturers have different device ID formats:

```python
def _get_serial_number_from_device_id(self, device_id: str, manufacturer: str) -> str:
    if manufacturer.lower() == "edgetech":
        return device_id.split("_")[0]  # "edgetech_ET-12345_..." → "ET-12345"
    return device_id  # Default: use full device_id
```

---

## Integration Flow

### Download Flow (RMW Hub → Earth Ranger)

```
┌─────────────────────────────────────────────────────────────┐
│ 1. INITIATE DOWNLOAD                                        │
│    - Calculate time window (90 days)                        │
│    - Build search_hub request                               │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. FETCH FROM RMW HUB                                       │
│    POST /search_hub/                                        │
│    - api_key authentication                                 │
│    - max_sets: 10000                                        │
│    - start_datetime_utc filter                              │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. PARSE RESPONSE                                           │
│    - Extract "sets" array                                   │
│    - Convert to GearSet objects                             │
│    - Convert nested traps to Trap objects                   │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. FETCH EARTH RANGER GEARS                                 │
│    GET /api/v1.0/gear/                                      │
│    - Create trap_id → gear mapping                          │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. FILTER AND PROCESS                                       │
│    For each trap in each gear set:                          │
│    ┌──────────────────────────────────────────────────────┐ │
│    │ Check if trap exists in ER                           │ │
│    │ ├─ Not exists + retrieved → SKIP                     │ │
│    │ ├─ Exists + same status → SKIP                       │ │
│    │ └─ Status change → CREATE OBSERVATION                │ │
│    └──────────────────────────────────────────────────────┘ │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. BUILD OBSERVATIONS                                       │
│    - Determine event_type from status                       │
│    - Calculate recorded_at timestamp                        │
│    - Include raw gear set data                              │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 7. SEND TO GUNDI                                            │
│    - Batch observations (100 per batch)                     │
│    - Send each batch to Gundi API                           │
│    - Log activity                                           │
└─────────────────────────────────────────────────────────────┘
```

### Upload Flow (Earth Ranger → RMW Hub)

```
┌─────────────────────────────────────────────────────────────┐
│ 1. INITIATE UPLOAD                                          │
│    - Log activity start                                     │
│    - Initialize update collection                           │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. STREAM HAULED GEARS                                      │
│    Async iterator over ER gears:                            │
│    - Filter: state="hauled"                                 │
│    - Filter: updated_after=start_datetime                   │
│    - Filter: source_type="ropeless_buoy"                    │
│    - Skip: manufacturer="rmwhub"                            │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. STREAM DEPLOYED GEARS                                    │
│    Async iterator over ER gears:                            │
│    - Filter: state="deployed"                               │
│    - Filter: updated_after=start_datetime                   │
│    - Skip: manufacturer="rmwhub"                            │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. TRANSFORM TO RMW FORMAT                                  │
│    For each ER gear:                                        │
│    ┌──────────────────────────────────────────────────────┐ │
│    │ Create Trap for each device                          │ │
│    │ - Extract serial number                              │ │
│    │ - Map status                                         │ │
│    │ - Set sequence and is_on_end                         │ │
│    └──────────────────────────────────────────────────────┘ │
│    ┌──────────────────────────────────────────────────────┐ │
│    │ Create GearSet                                       │ │
│    │ - id from gear UUID                                  │ │
│    │ - deployment_type from device count                  │ │
│    │ - when_updated_utc from last_updated                 │ │
│    └──────────────────────────────────────────────────────┘ │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. PREPARE UPLOAD PAYLOAD                                   │
│    - Transform field names (id → set_id, trap_id)          │
│    - Handle null release_type                              │
│    - Build upload_data structure                           │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. UPLOAD TO RMW HUB                                        │
│    POST /upload_deployments/                                │
│    - api_key authentication                                 │
│    - format_version: 0                                      │
│    - sets array                                             │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 7. PROCESS RESPONSE                                         │
│    - Extract trap_count                                     │
│    - Log failed_sets if any                                 │
│    - Log activity completion                                │
└─────────────────────────────────────────────────────────────┘
```

---

## Execution Schedules

### Schedule 1: Every 3 Minutes

```python
@crontab_schedule("*/3 * * * *")
async def action_pull_observations(integration, action_config):
    # Override sync window to 90 days
    action_config.minutes_to_sync = 90 * 24 * 60
    
    # Download from RMW Hub
    num_observations = await handle_download(...)
    
    # Upload to RMW Hub
    num_sets_updated = await handle_upload(...)
    
    return {
        "observations_downloaded": num_observations,
        "sets_updated": num_sets_updated
    }
```

**Configuration**:
- **Frequency**: Every 3 minutes (480 times/day)
- **Sync Window**: **90 days** (overridden from config)
- **Purpose**: Near real-time bidirectional sync

**Override Reasoning**:
```python
# Forcing to sync the whole period
# TODO: Removing this after figuring out the hauling problem
action_config.minutes_to_sync = 90 * 24 * 60
```

### Schedule 2: Daily at 12:10 AM

```python
@crontab_schedule("10 0 * * *")
async def action_pull_observations_24_hour_sync(integration, action_config):
    # Uses configured sync window (default 30 minutes)
    
    # Download from RMW Hub
    num_observations = await handle_download(...)
    
    # Upload to RMW Hub
    num_sets_updated = await handle_upload(...)
    
    return {
        "observations_downloaded": num_observations,
        "sets_updated": num_sets_updated
    }
```

**Configuration**:
- **Frequency**: Once daily
- **Sync Window**: Configured value (default 30 minutes)
- **Purpose**: Daily full sync as backup

---

## Multi-Destination Support

### Environment Mapping

The integration supports multiple Earth Ranger environments:

```python
class Environment(Enum):
    DEV = "Buoy Dev"
    STAGE = "Buoy Staging"
    PRODUCTION = "Buoy Prod"
    RF_1086 = "Buoy RF 1086 Dev"
```

### Processing per Destination

```python
connection_details = await gundi_client.get_connection_details(integration.id)

for destination in connection_details.destinations:
    environment = Environment(destination.name)
    er_token, er_destination = await get_er_token_and_site(integration, environment)
    
    # Create adapter for this destination
    rmw_adapter = RmwHubAdapter(
        integration.id,
        action_config.api_key,
        action_config.rmw_url,
        er_token,
        er_destination + "api/v1.0"
    )
    
    # Download and upload for this environment
    await handle_download(rmw_adapter, ...)
    await handle_upload(rmw_adapter, ...)
```

**Per-Destination Processing**:
- Separate `RmwHubAdapter` instance per destination
- Independent ER token and URL
- Isolated observations and uploads

---

## Data Examples

### Example 1: Single Trap Deployment

**RMW Hub Input**:
```json
{
  "sets": [{
    "vessel_id": "vessel_123",
    "set_id": "set_456",
    "deployment_type": "single",
    "when_updated_utc": "2025-10-20T10:00:00+00:00",
    "traps": [{
      "trap_id": "trap_789",
      "sequence": 1,
      "latitude": 42.123456,
      "longitude": -70.654321,
      "deploy_datetime_utc": "2025-10-20T08:00:00+00:00",
      "status": "deployed",
      "accuracy": "gps",
      "is_on_end": true
    }]
  }]
}
```

**Our Observation**:
```json
{
  "source_name": "set_456",
  "source": "trap_789",
  "location": {
    "lat": 42.123456,
    "lon": -70.654321
  },
  "recorded_at": "2025-10-20T08:00:00+00:00",
  "type": "ropeless_buoy",
  "subject_type": "ropeless_buoy_gearset",
  "additional": {
    "event_type": "trap_deployed",
    "raw": { /* Complete GearSet */ }
  }
}
```

### Example 2: Trawl Retrieval

**RMW Hub Input**:
```json
{
  "sets": [{
    "set_id": "set_789",
    "deployment_type": "trawl",
    "when_updated_utc": "2025-10-20T15:00:00+00:00",
    "traps": [
      {
        "trap_id": "trap_001",
        "sequence": 1,
        "latitude": 42.1,
        "longitude": -70.6,
        "deploy_datetime_utc": "2025-10-20T08:00:00+00:00",
        "retrieved_datetime_utc": "2025-10-20T14:00:00+00:00",
        "status": "retrieved",
        "is_on_end": false
      },
      {
        "trap_id": "trap_002",
        "sequence": 2,
        "latitude": 42.2,
        "longitude": -70.7,
        "deploy_datetime_utc": "2025-10-20T08:05:00+00:00",
        "retrieved_datetime_utc": "2025-10-20T14:05:00+00:00",
        "status": "retrieved",
        "is_on_end": true
      }
    ]
  }]
}
```

**Our Observations** (2 observations):
```json
[
  {
    "source_name": "set_789",
    "source": "trap_001",
    "location": {"lat": 42.1, "lon": -70.6},
    "recorded_at": "2025-10-20T14:00:00+00:00",
    "type": "ropeless_buoy",
    "subject_type": "ropeless_buoy_gearset",
    "additional": {
      "event_type": "trap_retrieved",
      "raw": { /* GearSet */ }
    }
  },
  {
    "source_name": "set_789",
    "source": "trap_002",
    "location": {"lat": 42.2, "lon": -70.7},
    "recorded_at": "2025-10-20T14:05:00+00:00",
    "type": "ropeless_buoy",
    "subject_type": "ropeless_buoy_gearset",
    "additional": {
      "event_type": "trap_retrieved",
      "raw": { /* GearSet */ }
    }
  }
]
```

### Example 3: Upload to RMW Hub

**Earth Ranger Gear**:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "display_id": "Gear-123",
  "name": "EdgeTech Buoy",
  "status": "deployed",
  "manufacturer": "edgetech",
  "last_updated": "2025-10-20T10:00:00+00:00",
  "devices": [
    {
      "device_id": "edgetech_ET-12345_a1b2c3d4_A",
      "source_id": "trap_001",
      "location": {"latitude": 42.1, "longitude": -70.6},
      "last_deployed": "2025-10-20T08:00:00+00:00",
      "last_updated": "2025-10-20T10:00:00+00:00"
    },
    {
      "device_id": "edgetech_ET-12345_a1b2c3d4_B",
      "source_id": "trap_002",
      "location": {"latitude": 42.2, "longitude": -70.7},
      "last_deployed": "2025-10-20T08:00:00+00:00",
      "last_updated": "2025-10-20T10:00:00+00:00"
    }
  ]
}
```

**RMW Hub Upload**:
```json
{
  "format_version": 0,
  "api_key": "***",
  "sets": [{
    "set_id": "550e8400-e29b-41d4-a716-446655440000",
    "vessel_id": "",
    "deployment_type": "trawl",
    "when_updated_utc": "2025-10-20T10:00:00+00:00",
    "traps": [
      {
        "trap_id": "trap_001",
        "sequence": 1,
        "latitude": 42.1,
        "longitude": -70.6,
        "deploy_datetime_utc": "2025-10-20T08:00:00+00:00",
        "retrieved_datetime_utc": null,
        "status": "deployed",
        "accuracy": "gps",
        "is_on_end": false,
        "manufacturer": "edgetech",
        "serial_number": "ET-12345"
      },
      {
        "trap_id": "trap_002",
        "sequence": 2,
        "latitude": 42.2,
        "longitude": -70.7,
        "deploy_datetime_utc": "2025-10-20T08:00:00+00:00",
        "retrieved_datetime_utc": null,
        "status": "deployed",
        "accuracy": "gps",
        "is_on_end": true,
        "manufacturer": "edgetech",
        "serial_number": "ET-12345"
      }
    ]
  }]
}
```

---

## Error Handling and Logging

### Download Errors

**Invalid JSON Response**:
```python
try:
    response_json = json.loads(response)
except json.JSONDecodeError:
    logger.error(f"Invalid JSON response: {response}")
    return []
```

**Missing Sets Field**:
```python
if "sets" not in response_json:
    logger.error(f"Failed to download data from RMW Hub API")
    return []
```

### Upload Errors

**No Gears Found**:
```python
if not rmw_updates:
    logger.info("No gear found in EarthRanger, skipping upload.")
    await log_action_activity(
        level=LogLevel.INFO,
        title="No gear found in EarthRanger, skipping upload."
    )
    return 0, {}
```

**Upload Failure**:
```python
if response.status_code != 200:
    logger.error(f"Upload failed with status {response.status_code}")
    await log_action_activity(
        level=LogLevel.ERROR,
        title=f"Upload failed with status {response.status_code}"
    )
    return 0, {}
```

**Failed Sets in Response**:
```python
failed_sets = result.get("failed_sets", [])
if failed_sets:
    logger.warning(f"Failed to upload {len(failed_sets)} sets: {failed_sets}")
    await log_action_activity(
        level=LogLevel.WARNING,
        title=f"Failed to upload {len(failed_sets)} sets",
        data={"failed_sets": failed_sets}
    )
```

### Processing Errors

**Individual Gear Processing**:
```python
try:
    rmw_update = await self._create_rmw_update_from_er_gear(er_gear)
    rmw_updates.append(rmw_update)
except Exception as e:
    logger.error(f"Error processing gear {er_gear.name}: {e}")
    # Continue processing other gears
```

### Activity Logging

**Download Activity**:
```python
await log_action_activity(
    integration_id=integration.id,
    action_id="pull_observations",
    level=LogLevel.INFO,
    title="Extracting observations with filter",
    data={
        "start_date_time": start_datetime.isoformat(),
        "end_date_time": end_datetime.isoformat(),
        "environment": str(environment),
        "gear_sets_to_process": len(rmw_sets)
    },
    config_data=action_config.dict()
)
```

**Upload Success**:
```python
await log_action_activity(
    integration_id=integration_uuid,
    action_id="pull_observations",
    level=LogLevel.INFO,
    title=f"Successfully uploaded {trap_count} traps to RMW Hub",
    data={"trap_count": trap_count}
)
```

---

## Performance Optimizations

### Streaming Gears

Instead of loading all gears into memory:

```python
async def iter_er_gears(self, start_datetime: datetime, state: str) -> AsyncIterator[BuoyGear]:
    params = {
        'updated_after': start_datetime.isoformat(),
        'source_type': SOURCE_TYPE,
        'page_size': 1000,
        'state': state
    }
    
    async for gear in self.gear_client.iter_gears(params=params):
        yield gear
```

**Benefits**:
- Memory-efficient for large datasets
- Processes gears one-by-one
- Pagination handled automatically
- Early termination possible

### Batching Observations

Observations sent to Gundi in batches:

```python
LOAD_BATCH_SIZE = 100

for batch in generate_batches(observations, n=100):
    await send_observations_to_gundi(
        observations=batch,
        integration_id=str(integration.id)
    )
```

**Benefits**:
- Prevents request payload limits
- Better error isolation
- Progress tracking

### Parallel Destination Processing

Each destination processed independently:

```python
for destination in connection_details.destinations:
    # Independent adapter instance
    # Separate download and upload
    # Isolated error handling
```

---

## API Reference

### RMW Hub Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/search_hub/` | POST | Download gear sets |
| `/upload_deployments/` | POST | Upload gear sets |

### Earth Ranger Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1.0/gear/` | GET | List existing gears |

### Request/Response Codes

| Code | Meaning | Action |
|------|---------|--------|
| 200 | OK | Process response data |
| 400 | Bad Request | Log error, skip item |
| 401 | Unauthorized | Check API key |
| 500 | Server Error | Retry or skip |

---

## Configuration Options

### Action Configuration

```python
class PullRmwHubObservationsConfiguration:
    api_key: SecretStr              # RMW Hub API key
    rmw_url: str                    # Default: "https://test.ropeless.network/api/"
    share_with: List[str]           # Entities to share data with
    minutes_to_sync: int            # Default: 30 (overridden to 90 days)
```

### Gear Client Timeouts

```python
RmwHubAdapter(
    gear_timeout=45.0,           # Total request timeout
    gear_connect_timeout=10.0,   # Connection timeout
    gear_read_timeout=45.0       # Read timeout
)
```

---

## Conclusion

This bidirectional integration provides robust synchronization between RMW Hub and our buoy tracking system:

✅ **Bidirectional sync** - Download from RMW Hub, upload our data back  
✅ **API key authentication** - Simple, persistent authentication  
✅ **Intelligent filtering** - Process only status changes  
✅ **Multi-destination support** - Handle multiple ER environments  
✅ **Memory-efficient streaming** - Process large datasets without memory issues  
✅ **Comprehensive error handling** - Continue processing despite individual failures  
✅ **Dual schedule** - High-frequency sync (3 min) + daily backup  
✅ **Activity logging** - Full audit trail in Gundi  

The system runs every 3 minutes with a 90-day sync window, ensuring comprehensive data sharing between platforms while avoiding circular uploads and duplicate processing.
