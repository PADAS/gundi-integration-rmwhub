from typing import List, Optional, Tuple, AsyncIterator

import hashlib
import json
import logging
import uuid
from datetime import datetime

import pytz
from gundi_core.schemas.v2.gundi import LogLevel
from erclient import ERClient
from app.services.activity_logger import log_action_activity
from ..buoy.client import BuoyClient
from ..buoy.types import BuoyGear
from .client import RmwHubClient
from .types import GearSet, Trap, SOURCE_TYPE

logger = logging.getLogger(__name__)


class RmwHubAdapter:
    def __init__(
        self,
        integration_id: str,
        api_key: str,
        rmw_url: str,
        er_token: str,
        er_destination: str,
        *args,
        **kwargs,
    ):
        self.integration_id = integration_id
        self.rmw_client = RmwHubClient(api_key, rmw_url)
        self.gear_client = BuoyClient(
            er_token, 
            er_destination,
            default_timeout=kwargs.get('gear_timeout', 45.0),
            connect_timeout=kwargs.get('gear_connect_timeout', 10.0),
            read_timeout=kwargs.get('gear_read_timeout', 45.0),
        )
        self.er_subject_name_to_subject_mapping = {}
        self.options = kwargs.get("options", {})

    @property
    def integration_uuid(self):
        """Get integration_id as a UUID object."""
        if isinstance(self.integration_id, uuid.UUID):
            return self.integration_id
        return uuid.UUID(self.integration_id)

    async def download_data(
        self, start_datetime: str, status: bool = None
    ) -> List[GearSet]:
        """
        Downloads data from the RMW Hub API using the search_hub endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        response = await self.rmw_client.search_hub(start_datetime, status)
        
        try:
            response_json = json.loads(response)
        except json.JSONDecodeError:
            logger.error(f"Failed to download data from RMW Hub API. Invalid JSON response: {response}")
            return []

        if "sets" not in response_json:
            logger.error(f"Failed to download data from RMW Hub API. Error: {response}")
            return []

        return self.convert_to_sets(response_json)

    def convert_to_sets(self, response_json: dict) -> List[GearSet]:

        if "sets" not in response_json:
            logger.error("Failed to download data from RMW Hub API.")
            return []

        sets = response_json["sets"]
        gearsets = []
        for gearset in sets:
            traps = []
            for trap in gearset["traps"]:
                trap_obj = Trap(
                    id=trap["trap_id"],
                    sequence=trap["sequence"],
                    latitude=trap["latitude"],
                    longitude=trap["longitude"],
                    deploy_datetime_utc=trap["deploy_datetime_utc"],
                    surface_datetime_utc=trap["surface_datetime_utc"],
                    retrieved_datetime_utc=trap["retrieved_datetime_utc"],
                    status=trap["status"],
                    accuracy=trap["accuracy"],
                    release_type=trap["release_type"],
                    is_on_end=trap["is_on_end"],
                )
                traps.append(trap_obj)

            gearset = GearSet(
                vessel_id=gearset["vessel_id"],
                id=gearset["set_id"],
                deployment_type=gearset["deployment_type"],
                traps_in_set=gearset.get("traps_in_set"),
                trawl_path=gearset["trawl_path"],
                share_with=gearset.get("share_with", []),
                when_updated_utc=gearset["when_updated_utc"],
                traps=traps,
            )

            gearsets.append(gearset)

        return gearsets

    async def process_download(self, rmw_sets: List[GearSet]) -> List:
        """
        Process the sets from the RMW Hub API.
        """
        # Normalize the extracted data into a list of observations following the new Data Model
        observations = []

        for gearset in rmw_sets:
            new_observations = await gearset.build_observations()
            observations.extend(new_observations)

        return observations


    async def iter_er_gears(self, start_datetime: datetime = None) -> AsyncIterator[BuoyGear]:
        """
        Iterate over gears from EarthRanger for the RMW Hub integration.
        
        This method uses an async generator for memory-efficient streaming
        of gears without loading all data into memory at once.
        
        Args:
            start_datetime: Filter gears updated after this datetime
            
        Yields:
            BuoyGear objects one at a time
        """
        # Build query parameters
        params = {}
        if start_datetime:
            params['updated_after'] = start_datetime.isoformat()
        params['source_type'] = SOURCE_TYPE
        
        async for gear in self.gear_client.iter_gears(params=params):
            yield gear

    async def process_upload(
        self,
        start_datetime: datetime,
    ) -> Tuple[int, dict]:
        """
        Process updates to be sent to the RMW Hub API.
        Returns the number of updates and the RMW Hub response.
        """
        # Start Activity Logs for the upload task
        logger.info("Starting upload task")
        upload_task_id = await log_action_activity(
            integration_id=self.integration_uuid,
            action_log_type="upload",
            level=LogLevel.INFO,
            log="Starting upload task",
        )

        try:
            # Use streaming approach for better memory efficiency
            rmw_updates = []
            errors = []
            gear_count = 0

            # Stream gears and process them one by one
            async for er_gear in self.iter_er_gears(start_datetime=start_datetime):
                gear_count += 1
                try:
                    rmw_update = await self._create_rmw_update_from_er_gear(er_gear, {})
                    if rmw_update:
                        rmw_updates.append(rmw_update)
                        logger.info(f"Processed gear {er_gear.name}")
                except Exception as e:
                    logger.error(f"Error processing gear {er_gear.name}: {e}")
                    errors.append((f"Error processing gear {er_gear.name}", e))

            logger.info(f"Found {gear_count} gears in EarthRanger")

            if not rmw_updates:
                logger.info("No gear found in EarthRanger, skipping upload.")
                await log_action_activity(
                    integration_id=self.integration_uuid,
                    action_log_type="upload",
                    level=LogLevel.INFO,
                    log="No gear found in EarthRanger, skipping upload.",
                    parent_log_id=upload_task_id,
                )
                return 0, {}

            if rmw_updates:
                try:
                    # Upload updates to RMW Hub
                    response = await self.rmw_client.upload_data(rmw_updates)
                    if response.status_code == 200:
                        response_data = response.json()
                        result = response_data.get("result", {})
                        trap_count = result.get("trap_count", 0)
                        failed_sets = result.get("failed_sets", [])
                        
                        # Log failed sets if any
                        if failed_sets:
                            logger.warning(f"Failed to upload {len(failed_sets)} sets: {failed_sets}")
                            await log_action_activity(
                                integration_id=self.integration_uuid,
                                action_log_type="upload",
                                level=LogLevel.WARNING,
                                log=f"Failed to upload {len(failed_sets)} sets: {failed_sets}",
                                parent_log_id=upload_task_id,
                            )
                        
                        logger.info(f"Successfully uploaded {trap_count} traps to RMW Hub")
                        await log_action_activity(
                            integration_id=self.integration_uuid,
                            action_log_type="upload",
                            level=LogLevel.INFO,
                            log=f"Successfully uploaded {trap_count} traps to RMW Hub",
                            parent_log_id=upload_task_id,
                        )

                        return trap_count, response_data
                    else:
                        logger.error(f"Upload failed with status {response.status_code}")
                        await log_action_activity(
                            integration_id=self.integration_uuid,
                            action_log_type="upload",
                            level=LogLevel.ERROR,
                            log=f"Upload failed with status {response.status_code}",
                            parent_log_id=upload_task_id,
                        )
                        return 0, {}
                except Exception as e:
                    logger.error(f"Upload error: {e}")
                    await log_action_activity(
                        integration_id=self.integration_uuid,
                        action_log_type="upload",
                        level=LogLevel.ERROR,
                        log=f"Upload error: {e}",
                        parent_log_id=upload_task_id,
                    )
                    return 0, {}

        except Exception as e:
            logger.error(f"Error in upload task: {e}")
            await log_action_activity(
                integration_id=self.integration_uuid,
                action_log_type="upload",
                level=LogLevel.ERROR,
                log=f"Error in upload task: {e}",
                parent_log_id=upload_task_id,
            )
            return 0, []

    async def _create_rmw_update_from_er_gear(
        self,
        er_gear: BuoyGear,
    ) -> Optional[GearSet]:
        """
        Create an RMW update from an EarthRanger gear.
        """
        traps = []
        for i, device in enumerate(er_gear.devices):
            traps.append(
                Trap(
                    id=device.device_id,
                    sequence=i + 1,
                    latitude=device.location.latitude,
                    longitude=device.location.longitude,
                    deploy_datetime_utc=device.last_deployed.isoformat() if device.last_deployed else None,
                    surface_datetime_utc=None,
                    retrieved_datetime_utc=device.last_updated.isoformat() if er_gear.status == "retrieved" else None,
                    status="deployed" if er_gear.status == "deployed" else "retrieved",
                    accuracy="unknown",
                    release_type=None,
                    is_on_end=False,
                )
            )
        gear_set = GearSet(
            vessel_id="unknown",
            id=str(er_gear.id),
            deployment_type="unknown",
            traps_in_set=len(traps),
            trawl_path="",
            share_with=[],
            traps=traps,
            when_updated_utc=er_gear.last_updated.isoformat(),
        )
        return gear_set

    async def create_display_id_to_gear_mapping(self, er_gears: List[BuoyGear]) -> dict:
        """
        Create a mapping of display IDs to gears for quick lookup.
        """
        mapping = {}
        for gear in er_gears:
            if gear.manufacturer == "rmwhub":
                continue  # Skip RMW Hub gears to avoid uploading their own data
            additional_data = gear.additional or {}
            display_id = additional_data.get("display_id")
            if display_id:
                mapping[display_id] = gear
        return mapping

    def validate_response(self, response: str) -> bool:
        """
        Validate the JSON response from the RMW Hub API.
        """
        if not response:
            logger.error("Empty response from RMW Hub API")
            return False

        try:
            json.loads(response)
            return True
        except json.JSONDecodeError:
            logger.error("Invalid JSON response from RMW Hub API")
            return False

    def clean_data(self, value: str) -> str:
        """
        Clean the data by removing special characters.
        """
        if not isinstance(value, str):
            return str(value)

        cleaned_str = (
            value.replace("\n", " ")
            .replace("\r", " ")
            .replace("\t", " ")
            .replace("'", "")
            .replace('"', "")
            .replace("  ", " ")
            .strip()
        )
        return cleaned_str

    def convert_datetime_to_utc(self, datetime_str: str) -> str:
        """
        Convert the datetime string to UTC format.
        """
        if datetime_str.endswith("Z"):
            datetime_str = datetime_str[:-1] + "+00:00"
        datetime_obj = datetime.fromisoformat(datetime_str)
        datetime_obj = datetime_obj.astimezone(pytz.utc)
        formatted_datetime = datetime_obj.isoformat()

        return formatted_datetime
