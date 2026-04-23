# RMW Hub Integration Documentation

## Overview

This document describes the bidirectional integration between RMW Hub (Ropeless Monitoring and Warning Hub) and our buoy tracking system (Earth Ranger destinations). It focuses on:

1. **RMW Hub API Connection** - Authentication and data synchronization mechanisms
2. **Data Structure** - Expected format from RMW Hub and our system
3. **Record Filtering** - Which gear records we process vs. discard (including duplicate trap_id handling)
4. **Download Output** - Gear payloads sent to the Buoy API (Earth Ranger); observation mapping is also documented for reference
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
  "max_sets": 1000,
  "start_datetime_utc": "2025-10-20T00:00:00+00:00"
}
```

**Parameters**:
- `format_version`: API format version (currently 0.1)
- `api_key`: Authentication key
- `max_sets`: Maximum number of gear sets to return per page (1000)
- `start_datetime_utc`: Filter sets updated after this timestamp

**Pagination**: The client automatically paginates through results by advancing `start_datetime_utc` to the latest `when_updated_utc` from each page. This continues until a page returns fewer than 1000 sets or a maximum of 20 pages is reached. Sets are deduplicated by `set_id` across pages.

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

**Multi-trap sets**: Sets with any number of traps (e.g. three device gearsets) are supported. The download flow parses all traps and builds one gear payload per set for the Buoy API. When any device in a set is marked for haul, the **whole set** is hauled in Buoy (all devices in one haul payload) so the gearset never ends up in a partial state.

#### trawl_path
trawl_path (object | null)
Optional path of the trawl deployment. This feature is still under development but will have
the following proposed rules:
* Trawl path as a LineString: `”trawl_path”: {“type”: “LineString”, “coordinates”: [[-70.1234, 42.5678], [-70.1200, 42.5700]]}` (GeoJSON coordinate order is `[longitude, latitude]`)
* First and last points of the trawl path array correspond to first and last trap locations of the gear set
* Trawl path points are updated as the vessel goes along the path
* Trawl path increment defined by distance with 100 m recommended as default increment
* If the status of either end of the gear set is changed to hauled, the entire gear set is hauled including the trawl path
* If start and/or end positions of gear set are updated such that it no longer corresponds to the trawl path, the trawl path is greyed out and straight (dashed) line is drawn between trawl end points

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

### Duplicate trap_id in a set

RMW Hub may occasionally return the same `trap_id` more than once in a set (e.g. duplicate rows or three device gearsets where one id is repeated). The Buoy API expects **one device per `device_id` per set**, so we normalize before building payloads:

- **Deduplication**: Before creating deployment or haul payloads, traps are deduplicated by `trap_id`. For each id, we keep a single representative trap.
- **Which trap we keep**: Among duplicates with the same `trap_id`, we keep the trap with the **lowest sequence** number; if sequences are equal, we keep the first occurrence.
- **Logging**: When any duplicate is collapsed, a **WARNING** is logged with the gear set id, `trap_id`, and how many entries were merged (e.g. *"deduplicated traps by trap_id; kept one of 2 entries for trap_id=..."*). This makes upstream data issues visible without failing the sync.

### Timestamp Priority

The `_create_gear_payload_from_gearset()` method determines `recorded_at` and `last_updated` for each device in the gear payload sent to the Buoy API.

**For Deployed Status**:
```python
recorded_at = deploy_datetime_utc or now()
last_updated = when_updated_utc if when_updated_utc > deploy_datetime_utc else deploy_datetime_utc
```

`recorded_at` always uses the actual deployment time because ER/Buoy uses it as the `assigned_range` lower bound. `last_updated` may use the gearset's `when_updated_utc` (when it's later than the deploy time) so that location-only or set-move updates are recognized by the API.

> **Why `recorded_at` must not use `when_updated_utc`**: ER/Buoy sets `assigned_range = [recorded_at, ...)` on deploy and `assigned_range = [..., recorded_at + 1s)` on haul. For trawls with very short deploy-to-retrieval windows, `when_updated_utc` (a metadata timestamp) can be later than `retrieved_datetime_utc` (the actual haul time). Inflating `recorded_at` to `when_updated_utc` on deploy would create an invalid range where `upper < lower`, leaving the device permanently stuck as "deployed."

**For Retrieved/Hauled Status**:
```python
recorded_at = retrieved_datetime_utc or surface_datetime_utc or haul_fallback_time or deploy_datetime_utc or now()
last_updated = recorded_at  # same priority chain
```

**Priority**: `retrieved` > `surfaced` > `haul_fallback` > `deployed` > `current time`

> **Legacy records and haul rejections**: Some ER gear created before the current `recorded_at` logic have `last_deployed` stamped with the sync-run time instead of `trap.deploy_datetime_utc`. When RMW Hub later marks such a set retrieved with a `retrieved_datetime_utc` that predates the ER sync time, the haul payload's `recorded_at` lands before ER's `last_deployed`, producing an invalid `assigned_range` (upper < lower) and the Buoy API rejects the haul with **HTTP 500: `range lower bound must be less than or equal to range upper bound`** (the underlying PostgreSQL `tstzrange` error). Example: set `819B84A8-0DAF-4B3B-B4C1-EE2174B31EAA` — ER `last_deployed` = 2025-12-15T20:03:31Z, RMW Hub `retrieved_datetime_utc` = 2025-11-20T16:57:05Z. New records created by the current adapter are not affected; to unstick a legacy record, either bump RMW Hub's `retrieved_datetime_utc` forward or nudge ER's `last_deployed` back.

---

## Record Filtering

### Download Filtering (RMW Hub → Our System)

#### Records We Process

✅ **Included Records**
- All gear sets returned by RMW Hub for the time window
- Traps with valid status ("deployed" or "retrieved")
- Traps with valid location data (latitude/longitude)

✅ **Status-Based Processing**

Download produces **gear payloads** for the Buoy API (Earth Ranger). For each set we may emit up to two payloads: one for deployment and one for haul, depending on which traps need syncing.

**Whole-gearset haul**: For gearsets coming from RMW Hub, if **any** device in the set is marked for haul (status `retrieved`), we haul the **entire** gearset in Buoy. We send one haul payload that includes every trap in the set, not only the ones RMW marks as retrieved. Traps that RMW still shows as `deployed` (e.g. due to sync error) are included in that haul payload and use a fallback haul time (latest retrieved/surface time in the set, or the gearset’s `when_updated_utc`). We do **not** emit a deployment payload for that set when we emit a haul. This keeps Buoy consistent: one haul per set when any device is hauled, avoiding partial states (one device hauled, one still deployed).

| RMW Hub Status | ER/Buoy State | Action |
|----------------|---------------|--------|
| `deployed` | Not exists | Include in deployment payload |
| `deployed` | `deployed` (same location) | Skip (already in sync) |
| `deployed` | `hauled` | Include in deployment payload (re-deployment) |
| `retrieved` | Not exists | Skip (no deployment history; log) |
| `retrieved` | `deployed` | Trigger **whole-gearset haul** (all traps in set) |
| `retrieved` | `hauled` (same location) | Skip (already in sync) |

#### Records We Discard

❌ **Excluded During Processing**

| Condition | Reason | Log Level |
|-----------|--------|-----------|
| Invalid set_id or trap_id (non-UUID or nil/reserved zero-prefixed UUID) | Cannot sync to Buoy API | WARNING |
| Retrieved trap not in ER | No deployment history | INFO |
| Same status and location in ER | Already synchronized | INFO |
| Duplicate trap_id in same set | Collapsed to one device per id (see below) | WARNING |

**Duplicate trap_id in a set**: If a set has multiple traps sharing the same `trap_id` (e.g. RMW Hub duplicate rows or three traps with one id repeated), we deduplicate before building the gear payload: we keep one trap per `trap_id` (lowest sequence), log a warning, and send a payload with unique `device_id`s only. This ensures the Buoy API never receives duplicate device IDs in a single set.

**Filter Implementation** (overview):
```python
async def process_download(self, rmw_sets: List[GearSet]) -> List[Dict]:
    gears = await self.gear_client.get_all_gears(page_size=ER_GEAR_PAGE_SIZE)
    gear_id_to_set_mapping = {str(gear.id).lower(): gear for gear in gears}

    gear_payloads = []
    for gearset in rmw_sets:
        er_gear = gear_id_to_set_mapping.get(str(gearset.id).lower())
        traps_to_deploy = []   # traps with status "deployed"
        traps_to_haul = []     # traps with status "retrieved"

        for trap in gearset.traps:
            # Skip retrieved traps with no ER gear
            if not er_gear and trap.status == "retrieved":
                continue
            # Skip if ER device exists and status + location match
            if er_gear and matching_device_found(er_gear, trap):
                continue
            if trap.status == "deployed":
                traps_to_deploy.append(trap)
            elif trap.status == "retrieved":
                traps_to_haul.append(trap)

        # Deduplicate by trap_id so payload has unique device_id per set
        traps_to_deploy, _ = deduplicate_traps_by_id(traps_to_deploy)
        traps_to_haul, _ = deduplicate_traps_by_id(traps_to_haul)

        # Deploy only when we are not hauling this set (whole-gearset haul takes precedence)
        if traps_to_deploy and not traps_to_haul:
            gear_payloads.append(_create_gear_payload_from_gearset(gearset, traps_to_deploy, "deployed"))
        # If any device is marked for haul, haul the whole gearset (all traps in set)
        if traps_to_haul:
            all_traps_deduped, _ = deduplicate_traps_by_id(gearset.traps)
            haul_fallback_time = _latest_haul_time_iso(all_traps_deduped, gearset.when_updated_utc)
            gear_payloads.append(_create_gear_payload_from_gearset(
                gearset, all_traps_deduped, "hauled", haul_fallback_time_utc=haul_fallback_time
            ))

    return gear_payloads  # Sent to Buoy API via send_gear_to_buoy_api()
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
    
    # Upload to RMW Hub in batches of 5
    batch_size = 5
    total_trap_count = 0
    all_failed_sets = []
    for i in range(0, len(rmw_updates), batch_size):
        batch = rmw_updates[i:i + batch_size]
        response = await self.rmw_client.upload_data(batch)
        if response.status_code == 200:
            result = response.json().get("result", {})
            total_trap_count += result.get("trap_count", 0)
            all_failed_sets.extend(result.get("failed_sets", []))
        else:
            all_failed_sets.extend([str(s.id) for s in batch])
    return total_trap_count, {"result": {"failed_sets": all_failed_sets, "trap_count": total_trap_count}}
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
│ 2. FETCH FROM RMW HUB (paginated)                            │
│    POST /search_hub/                                        │
│    - api_key authentication                                 │
│    - max_sets: 1000 per page                                │
│    - start_datetime_utc filter                              │
│    - Advance start_datetime_utc between pages               │
│    - Deduplicate sets by set_id across pages                │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. PARSE RESPONSE                                           │
│    - Extract combined "sets" array from all pages           │
│    - Convert to GearSet objects                             │
│    - Convert nested traps to Trap objects                   │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. FETCH EARTH RANGER GEARS                                 │
│    GET /api/v1.0/gear/ (paginated)                         │
│    - Create set_id → gear mapping for status/location check  │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. FILTER AND BUILD LISTS                                   │
│    For each trap in each gear set:                          │
│    ┌──────────────────────────────────────────────────────┐ │
│    │ Check ER gear by set_id; match device by trap_id     │ │
│    │ ├─ Not in ER + retrieved → SKIP                      │ │
│    │ ├─ In ER + same status & location → SKIP             │ │
│    │ └─ Otherwise → add to traps_to_deploy or traps_to_haul│
│    └──────────────────────────────────────────────────────┘ │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. DEDUPLICATE BY TRAP_ID                                   │
│    - For each set, collapse duplicate trap_id entries       │
│    - Keep one trap per trap_id (lowest sequence)             │
│    - Log WARNING when duplicates are collapsed             │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 7. BUILD GEAR PAYLOADS                                      │
│    - If any trap is to be hauled → haul **whole gearset**   │
│      (all traps in set, one haul payload; no deploy)        │
│    - Else if traps to deploy → one deploy payload           │
│    - Haul fallback time for traps without retrieved time    │
│    - devices_in_set, devices[] with unique device_id         │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│ 8. SEND TO BUOY API                                         │
│    POST /api/v1.0/gear/ for each payload                    │
│    - Log success/failure per payload                         │
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
        'page_size': ER_GEAR_PAGE_SIZE,
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

### Earth Ranger / Buoy API Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1.0/gear/` | GET | List existing gears (paginated) |
| `/api/v1.0/gear/` | POST | Create or update gear (gear payload from download) |

### Request/Response Codes

| Code | Meaning | Action |
|------|---------|--------|
| 200 | OK | Process response data |
| 400 | Bad Request | Log error, skip item |
| 401 | Unauthorized | Check API key |
| 502 | Bad Gateway | Retry up to 3 times (5s delay) |
| 503 | Service Unavailable | Retry up to 3 times (5s delay) |
| 504 | Gateway Timeout | Retry up to 3 times (5s delay) |
| 500 | Server Error | Log error, skip |

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

### RMW Hub Client Timeouts

```python
RmwHubAdapter(
    rmw_timeout=120.0,                # Total request timeout (search_hub)
    rmw_connect_timeout=10.0,         # Connection timeout (search_hub)
    rmw_read_timeout=120.0,           # Read timeout (search_hub)
    rmw_upload_timeout=300.0,         # Total request timeout (upload_data)
    rmw_upload_connect_timeout=10.0,  # Connection timeout (upload_data)
    rmw_upload_read_timeout=300.0     # Read timeout (upload_data)
)
```

Uploads use a longer timeout (5 minutes) and smaller batch sizes (5 gear sets per request) to avoid hanging on slow RMW Hub responses. Both `search_hub` and `upload_data` retry up to 3 times on 502/503/504 responses with a 5-second delay between attempts. The `search_hub_all` method paginates automatically (1000 sets per page, up to 20 pages) to avoid timeouts on large result sets.

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

✅ **Bidirectional sync** - Download from RMW Hub (gear payloads to Buoy API), upload our data back  
✅ **API key authentication** - Simple, persistent authentication  
✅ **Intelligent filtering** - Process only status/location changes; skip in-sync traps  
✅ **Whole-gearset haul** - When any device in an RMW Hub set is retrieved, haul the entire set in Buoy (avoids partial haul state)  
✅ **Duplicate trap_id handling** - Collapse duplicate trap_ids per set (keep one per id, log warning) so Buoy API always receives unique device_id per set  
✅ **Multi-trap sets** - Supports sets with any number of traps (e.g. three device gearsets)  
✅ **Multi-destination support** - Handle multiple ER environments  
✅ **Memory-efficient streaming** - Process large datasets without memory issues  
✅ **Comprehensive error handling** - Continue processing despite individual failures  
✅ **Transient failure resilience** - Automatic retries on 502/503/504 for both downloads and uploads  
✅ **Dual schedule** - High-frequency sync (3 min) + daily backup  
✅ **Activity logging** - Full audit trail in Gundi  

The system runs every 3 minutes with a 90-day sync window, ensuring comprehensive data sharing between platforms while avoiding circular uploads and duplicate processing.
