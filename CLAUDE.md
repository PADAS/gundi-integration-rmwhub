# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Gundi integration service that provides **bidirectional synchronization** between RMW Hub (Ropeless Monitoring and Warning Hub) and EarthRanger's buoy tracking system. The integration runs on scheduled intervals to download gear deployment data from RMW Hub, process it, and upload EarthRanger gear data back to RMW Hub.

**Key Integration Points:**
- Downloads gear sets and trap data from RMW Hub API (`search_hub` endpoint)
- Transforms RMW Hub data into EarthRanger Gear API payloads
- Uploads EarthRanger gear data back to RMW Hub (`upload_deployments` endpoint)
- Supports multiple EarthRanger destinations (Dev, Staging, Production, RF 1086 Dev)

## Development Commands

### Running the Application

**Local Development (Docker):**
```bash
cd docker
docker-compose up --build
```

**Local Development (Direct):**
```bash
# Install dependencies
pip install -r requirements.txt

# Run the FastAPI server
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

**Debug Mode with Docker:**
The devimage target includes debugpy on port 5678 for remote debugging.

### Testing

**Run all tests:**
```bash
pytest
```

**Run specific test file:**
```bash
pytest app/actions/tests/test_rmwhub_adapter.py
```

**Run tests with coverage:**
```bash
pytest --cov=app/actions --cov-report=html
```

**Test configuration (setup.cfg):**
- Test paths: `app/actions/tests`
- Coverage source: `app/actions`
- Test markers: `unit`, `integration`, `slow`

### Registration

Register the integration with Gundi:
```bash
python -m app.register --slug <integration-slug> --service-url <url> --schedule "action_id:cron_expression"
```

Example:
```bash
python -m app.register --slug rmwhub --service-url https://example.com --schedule "pull_observations:*/3 * * * *"
```

## Architecture

### Core Components

**1. Action Handlers (`app/actions/handlers.py`)**
- `action_pull_observations`: Main scheduled action (every 3 minutes) with 90-day sync window
- `action_pull_observations_24_hour_sync`: Daily backup sync (12:10 AM) with configurable window
- `handle_download`: Downloads from RMW Hub and transforms to gear payloads
- `handle_upload`: Uploads EarthRanger gear data to RMW Hub

**2. RMW Hub Adapter (`app/actions/rmwhub/adapter.py`)**
- `RmwHubAdapter`: Central orchestration layer bridging RMW Hub and EarthRanger
- `download_data`: Fetches gear sets from RMW Hub API
- `process_download`: Converts RMW Hub data to EarthRanger gear payloads
- `process_upload`: Streams EarthRanger gears and uploads to RMW Hub
- `send_gear_to_buoy_api`: Sends gear payloads directly to Buoy API

**3. Buoy Client (`app/actions/buoy/client.py`)**
- `BuoyClient`: HTTP client for EarthRanger Gear API
- `iter_gears`: Async generator for streaming gears (memory-efficient pagination)
- `get_all_gears`: Fetches all gears (use cautiously with large datasets)
- Configurable timeouts for long-running operations

**4. RMW Hub Client (`app/actions/rmwhub/client.py`)**
- `RmwHubClient`: HTTP client for RMW Hub API
- `search_hub`: Downloads gear sets with start_datetime filter
- `upload_data`: Uploads transformed gear data back to RMW Hub

### Data Models

**RMW Hub Models (`app/actions/rmwhub/types.py`):**
- `GearSet`: Container for a deployment (single trap or trawl line)
- `Trap`: Individual buoy/trap with location, status, and timestamps
- Key fields: `status` ("deployed" | "retrieved"), `deploy_datetime_utc`, `retrieved_datetime_utc`

**EarthRanger Models (`app/actions/buoy/types.py`):**
- `BuoyGear`: Gear set in EarthRanger with devices
- `BuoyDevice`: Individual device with location and deployment info
- `Environment`: Enum for destinations (DEV, STAGE, PRODUCTION, RF_1086)

### Data Flow

**Download Flow (RMW Hub → EarthRanger):**
1. Fetch gear sets from RMW Hub with `start_datetime` filter
2. Transform each trap into EarthRanger gear payload format
3. Send payloads directly to Buoy API endpoint
4. Track success/failure counts and log activity

**Upload Flow (EarthRanger → RMW Hub):**
1. Stream hauled gears from EarthRanger (state="hauled")
2. Stream deployed gears from EarthRanger (state="deployed")
3. Skip gears with manufacturer="rmwhub" (prevent circular uploads)
4. Transform each EarthRanger gear to RMW Hub GearSet format
5. Upload batch to RMW Hub API

**Key Filtering Logic:**
- Download: Only process traps with status changes (skip if already synced)
- Upload: Exclude manufacturer="rmwhub" to avoid circular data flow
- Both: Use start_datetime to fetch only recently updated records

### Multi-Destination Support

The integration processes each EarthRanger destination independently:
- Fetches connection details from Gundi client
- Creates separate `RmwHubAdapter` per destination with unique ER token/URL
- Runs download and upload operations for each destination
- Returns aggregated results per destination

## Important Implementation Details

### Scheduled Actions

**Every 3 Minutes (`action_pull_observations`):**
- **Sync Window:** 90 days (hardcoded override: `action_config.minutes_to_sync = 90 * 24 * 60`)
- **Why:** Temporary workaround for "hauling problem" (see TODO in handlers.py:176)
- Runs bidirectional sync (download + upload)

**Daily at 12:10 AM (`action_pull_observations_24_hour_sync`):**
- Uses configured sync window (default: 30 minutes)
- Backup sync mechanism

### Authentication

- **RMW Hub:** API key passed in request body
- **EarthRanger:** Bearer token in Authorization header
- Credentials stored in `AuthenticateConfig` (separate from action configs)
- Retrieved via `find_config_for_action(integration.configurations, "auth")`

### Timeout Configuration

The `BuoyClient` uses custom timeouts for long-running gear operations:
```python
RmwHubAdapter(...,
    gear_timeout=45.0,           # Total request timeout
    gear_connect_timeout=10.0,   # Connection timeout
    gear_read_timeout=45.0       # Read timeout
)
```

### Error Handling

- **Individual gear failures don't stop batch processing**
- Track success/failure counts separately
- Log activity with appropriate levels (INFO, WARNING, ERROR)
- Continue processing remaining gears on partial failures

### Circular Upload Prevention

When uploading to RMW Hub, the adapter skips gears where `manufacturer == "rmwhub"` to prevent re-uploading data that originally came from RMW Hub.

## Configuration Files

- **`.env.example`**: Environment variables template (Gundi API, Keycloak, Redis)
- **`setup.cfg`**: pytest and coverage configuration
- **`requirements.txt`**: Python dependencies (FastAPI, gundi-client-v2, earthranger-client)
- **`docker/Dockerfile`**: Multi-stage build (baseimage, devimage, prodimage)

## FastAPI Service Structure

**Main Application (`app/main.py`):**
- FastAPI app with CORS middleware
- PubSub message handler for triggering actions
- Auto-registration on startup if `REGISTER_ON_START=true`
- Health check endpoint: `GET /`

**Routers:**
- `/v1/actions`: Action execution endpoints
- `/webhooks`: Webhook receivers
- `/config-events`: Configuration event handlers

**Services (`app/services/`):**
- `action_runner.py`: Execute actions by ID
- `action_scheduler.py`: Crontab schedule decorators
- `activity_logger.py`: Log action execution to Gundi
- `self_registration.py`: Register integration type with Gundi

## Testing Best Practices

- Tests are in `app/actions/tests/`
- Use factories (`factories.py`) for creating test fixtures
- Mock external HTTP calls with `respx` library
- Test files follow naming: `test_<module>.py`
- Use pytest markers for test categorization: `@pytest.mark.unit`, `@pytest.mark.integration`

## Detailed Documentation

The README.md contains extensive documentation on:
- RMW Hub API endpoints and data structures
- Field mappings between systems
- Observation transformation logic
- Record filtering rules
- Error handling patterns
- Performance optimizations

Refer to README.md for complete integration flow diagrams and data examples.
