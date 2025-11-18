import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from gundi_client_v2 import GundiClient
from gundi_core.events import LogLevel
from gundi_core.schemas.v2 import Integration

from app.services.action_scheduler import crontab_schedule
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.utils import find_config_for_action

from .configurations import AuthenticateConfig, PullRmwHubObservationsConfiguration
from .rmwhub import RmwHubAdapter
from .buoy.types import Dict, Environment

logger = logging.getLogger(__name__)

async def action_auth(integration: Integration, action_config: AuthenticateConfig):
    logger.info(
        f"Executing auth action with integration {integration} and action_config {action_config}..."
    )

    api_key_is_valid = action_config.api_key is not None

    return {
        "valid_credentials": api_key_is_valid,
        "some_message": "something informative.",
    }


async def handle_download(
    rmw_adapter: RmwHubAdapter,
    start_datetime: datetime,
    end_datetime: datetime,
    integration: Integration,
    environment: Environment,
    action_config: PullRmwHubObservationsConfiguration
) -> Dict:
    logger.info(
        f"Downloading data from RMW Hub API...For the datetimes: {start_datetime.isoformat()} - {end_datetime.isoformat()}"
    )
    rmw_sets = await rmw_adapter.download_data(start_datetime)
    logger.info(
        f"{len(rmw_sets)} Gearsets Downloaded from RMW Hub API...For the datetimes: {start_datetime.isoformat()} - {end_datetime.isoformat()}"
    )

    await log_action_activity(
        integration_id=integration.id,
        action_id="pull_observations",
        level=LogLevel.INFO,
        title="Extracting observations with filter..",
        data={
            "start_date_time": start_datetime.isoformat(),
            "end_date_time": end_datetime.isoformat(),
            "environment": str(environment),
            "gear_sets_to_process": len(rmw_sets),
        },
        config_data=action_config.dict(),
    )

    if len(rmw_sets) == 0:
        await log_action_activity(
            integration_id=integration.id,
            action_id="pull_observations",
            level=LogLevel.INFO,
            title="No gearsets returned from RMW Hub API.",
            data={
                "start_date_time": start_datetime.isoformat(),
                "end_date_time": end_datetime.isoformat(),
                "environment": str(environment),
            },
            config_data=action_config.dict(),
        )
        return 0

    logger.info(
        f"Processing updates from RMW Hub API...Number of gearsets returned: {len(rmw_sets)}"
    )
    gear_payloads = await rmw_adapter.process_download(rmw_sets)
    logger.info(f"Created {len(gear_payloads)} gear payloads to send to Buoy API")
    
    # Send gear payloads directly to Buoy API and track results
    success_count = 0
    failure_count = 0
    failed_payloads = []
    
    for idx, payload in enumerate(gear_payloads):
        logger.info(f"Sending gear payload {idx + 1}/{len(gear_payloads)} to Buoy API")
        result = await rmw_adapter.send_gear_to_buoy_api(payload)
        if result.get("status") == "success":
            success_count += 1
            logger.info(f"Successfully sent gear set {idx + 1}/{len(gear_payloads)} to Buoy API")
        else:
            failure_count += 1
            error_info = result.get("error") or result.get("response", "Unknown error")
            logger.error(
                f"Failed to send gear set {idx + 1}/{len(gear_payloads)} to Buoy API: {error_info}"
            )
            failed_payloads.append({"index": idx, "error": error_info})
    
    # Log activity with success/failure counts
    log_level = LogLevel.INFO if failure_count == 0 else LogLevel.WARNING
    title = (
        f"Processed {len(gear_payloads)} gear sets: "
        f"{success_count} successful, {failure_count} failed"
    )
    await log_action_activity(
        integration_id=integration.id,
        action_id="pull_observations",
        level=log_level,
        title=title,
        data={
            "total": len(gear_payloads),
            "success": success_count,
            "failures": failure_count,
            "gear_sets_extracted": len(rmw_sets),
        },
        config_data=action_config.dict(),
    )
    
    return {
        "total": len(gear_payloads),
        "success": success_count,
        "failures": failure_count,
        "failed_payloads": failed_payloads if failed_payloads else None,
    }


async def handle_upload(
    rmw_adapter: RmwHubAdapter, start_datetime: datetime, integration: Integration, action_config: PullRmwHubObservationsConfiguration
):
    (
        num_saved_sets,
        rmw_response,
    ) = await rmw_adapter.process_upload(start_datetime)

    if rmw_response and "detail" in rmw_response:
        await log_action_activity(
            integration_id=integration.id,
            action_id="pull_observations",
            level=LogLevel.ERROR,
            title="Failed to upload data to rmwHub.",
            data={
                "rmw_response": str(rmw_response),
            },
            config_data=action_config.dict(),
        )
        return 0

    await log_action_activity(
        integration_id=integration.id,
        action_id="pull_observations",
        level=LogLevel.INFO,
        title="Process upload to rmwHub completed.",
        data={
            "rmw_response": str(rmw_response),
        },
        config_data=action_config.dict(),
    )
    return num_saved_sets


@activity_logger()
@crontab_schedule("*/3 * * * *")  # Run every 3 minutes
async def action_pull_observations(
    integration, action_config: PullRmwHubObservationsConfiguration
):
    current_datetime = datetime.now(timezone.utc)
    #! Forcing to sync the whole period
    # TODO: Removing this after figuring out the hauling problem
    action_config.minutes_to_sync = 90 * 24 * 60  # 90 days in minutes
    sync_interval_minutes = action_config.minutes_to_sync
    start_datetime = current_datetime - timedelta(minutes=sync_interval_minutes)
    end_datetime = current_datetime

    _client = GundiClient()
    connection_details = await _client.get_connection_details(integration.id)
    
    # Get the auth config from the integration (not from action_config)
    auth_config_data = find_config_for_action(
        configurations=integration.configurations, action_id="auth"
    )
    auth_config = AuthenticateConfig.parse_obj(auth_config_data.data)
    er_token = auth_config.er_token.get_secret_value()
    
    destination_result = {}
    
    for destination in connection_details.destinations:
        environment = Environment(destination.name)
        
        # Get destination base URL
        destination_details = await _client.get_integration_details(destination.id)
        er_destination = destination_details.base_url

        logger.info(
            f"Downloading data from rmwHub to the Earthranger destination: {str(environment)}..."
        )

        rmw_adapter = RmwHubAdapter(
            integration.id,
            action_config.api_key.get_secret_value(),
            action_config.rmw_url,
            er_token,
            er_destination + "api/v1.0"
        )

        download_result = await handle_download(rmw_adapter, start_datetime, end_datetime, integration, environment, action_config)
        num_sets = await handle_upload(rmw_adapter, start_datetime, integration, action_config)
        
        destination_key = f"{destination.id}_{destination.name}"
        destination_result[destination_key] = {
            "gear_payloads_total": download_result["total"],
            "gear_payloads_successful": download_result["success"],
            "gear_payloads_failed": download_result["failures"],
            "sets_updated_in_rmwhub": num_sets,
        }

    return destination_result

@activity_logger()
@crontab_schedule("10 0 * * *")  # Run every 24 hours at 12:10 AM
async def action_pull_observations_24_hour_sync(
    integration, action_config: PullRmwHubObservationsConfiguration
):
    current_datetime = datetime.now(timezone.utc)
    sync_interval_minutes = action_config.minutes_to_sync
    start_datetime = current_datetime - timedelta(minutes=sync_interval_minutes)
    end_datetime = current_datetime

    _client = GundiClient()
    connection_details = await _client.get_connection_details(integration.id)
    
    # Get the auth config from the integration (not from action_config)
    auth_config_data = find_config_for_action(
        configurations=integration.configurations, action_id="auth"
    )
    auth_config = AuthenticateConfig.parse_obj(auth_config_data.data)
    er_token = auth_config.er_token.get_secret_value()
    
    destination_result = {}
    
    for destination in connection_details.destinations:
        environment = Environment(destination.name)
        
        # Get destination base URL
        destination_details = await _client.get_integration_details(destination.id)
        er_destination = destination_details.base_url

        logger.info(
            f"Downloading data from rmwHub to the Earthranger destination: {str(environment)}..."
        )

        rmw_adapter = RmwHubAdapter(
            integration.id,
            action_config.api_key.get_secret_value(),
            action_config.rmw_url,
            er_token,
            er_destination + "api/v1.0"
        )

        download_result = await handle_download(rmw_adapter, start_datetime, end_datetime, integration, environment, action_config)
        num_sets = await handle_upload(rmw_adapter, start_datetime, integration, action_config)
        
        destination_key = f"{destination.id}_{destination.name}"
        destination_result[destination_key] = {
            "gear_payloads_total": download_result["total"],
            "gear_payloads_successful": download_result["success"],
            "gear_payloads_failed": download_result["failures"],
            "sets_updated_in_rmwhub": num_sets,
        }

    return destination_result