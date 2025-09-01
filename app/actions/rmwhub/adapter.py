from typing import List, Optional, Tuple

import hashlib
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum

import backoff
import httpx
import pytz
from dateparser import parse as parse_date
from fastapi.encoders import jsonable_encoder
from gundi_core.schemas.v2.gundi import LogLevel
from erclient import ERClient
from app.services.activity_logger import log_action_activity
from ..buoy.client import BuoyClient
from ..buoy.types import BuoyGear
from .client import RmwHubClient
from .types import GearSet, Trap, SOURCE_TYPE, SUBJECT_SUBTYPE, GEAR_DEPLOYED_EVENT, GEAR_RETRIEVED_EVENT, EPOCH

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
        self.er_client = ERClient(service_root = er_destination, token = er_token)
        self.gear_client = BuoyClient(er_token, er_destination)  # Updated to use BuoyClient
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
        response_json = json.loads(response)

        if "sets" not in response_json:
            logger.error(f"Failed to download data from RMW Hub API. Error: {response}")
            return []

        return self.convert_to_sets(response_json)

    @backoff.on_exception(
        backoff.expo, (httpx.ReadTimeout, httpx.ConnectTimeout), max_tries=5
    )
    async def _get_newest_set_from_rmwhub(self, trap_list):
        """
        Downloads data from the RMW Hub API using the search_own endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        if not trap_list:
            return None

        sets = await self.search_own(trap_id = trap_list[0], status = "deployed")

        newest = None
        newestDate = None
        for gearset in sets:
            set_traps = sorted([trap.id for trap in gearset.traps])
            if set_traps == trap_list:
                datecomp = parse_date(gearset.when_updated_utc)
                if not newestDate or (datecomp > newestDate):
                    newest = gearset
                    newestDate = datecomp

        return newest

    async def search_own(self, trap_id=None, status = None) -> dict:
        """
        Downloads data from the RMWHub API using the search_own endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        url = self.rmw_client.rmw_url + "/search_own/"

        data = {"format_version": 0.1, "api_key": self.rmw_client.api_key}

        if trap_id:
            data["trap_id"] = trap_id

        if status:
            data["status"] = status

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=RmwHubClient.HEADERS, json=data)

        if response.status_code != 200:
            logger.error(
                f"Failed to download data from RMW Hub API. Error: {response.status_code} - {response.text}"
            )
            return []

        try:
            return self.convert_to_sets(response.json())
        except (ValueError, json.JSONDecodeError) as e:
            logger.error(f"Failed to parse JSON response from RMW Hub API: {e}")
            return []

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

        # Normalize the extracted data into a list of observations following to the Gundi schema:
        observations = []

        for gearset in rmw_sets:
            new_observations = await gearset.create_observations()
            observations.extend(new_observations)

        return observations

    def _create_traps_gearsets_mapping_key(self, traps_ids: List[str]) -> str:
        """
        Create a unique key for the traps and gearsets mapping.
        """
        trap_ids_sorted = sorted(traps_ids)
        concat_devices = "_".join(trap_ids_sorted)

        # Generate a short hash based on the sorted device IDs
        display_id_hash = hashlib.sha256(str(concat_devices).encode()).hexdigest()[:12]

        return display_id_hash

    async def get_er_gears(self, start_datetime: datetime = None) -> List[BuoyGear]:
        """
        Get gears from EarthRanger for the RMW Hub integration.
        """
        return await self.gear_client.get_gears(
            start_datetime=start_datetime, source_type=SOURCE_TYPE
        )

    async def process_upload(
        self,
        start_datetime: datetime,
        previous_start_datetime: datetime = None,
        integration_logger=None,
    ) -> Tuple[
        List[datetime], List[datetime], List[datetime], List[Tuple[str, Exception]]
    ]:
        """
        Process updates to be sent to the RMW Hub API.
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
            # Get updated gears from EarthRanger
            er_gears = await self.get_er_gears(start_datetime=start_datetime)
            logger.info(f"Found {len(er_gears)} gears in EarthRanger")

            if not er_gears:
                logger.info("No gear found in EarthRanger, skipping upload.")
                await log_action_activity(
                    integration_id=self.integration_uuid,
                    action_log_type="upload",
                    level=LogLevel.INFO,
                    log="No gear found in EarthRanger, skipping upload.",
                    parent_log_id=upload_task_id,
                )
                return [], [], [], []

            # Create a mapping of display IDs to gears for quick lookup
            display_id_to_gear_mapping = await self.create_display_id_to_gear_mapping(
                er_gears
            )

            # Process each gear and create RMW updates
            rmw_updates = []
            processed_times = []
            success_times = []
            error_times = []
            errors = []

            for er_gear in er_gears:
                try:
                    rmw_update = await self._create_rmw_update_from_er_gear(
                        er_gear, display_id_to_gear_mapping
                    )
                    if rmw_update:
                        rmw_updates.append(rmw_update)
                        processed_times.append(datetime.now())
                        logger.info(f"Processed gear {er_gear.name}")
                except Exception as e:
                    logger.error(f"Error processing gear {er_gear.name}: {e}")
                    error_times.append(datetime.now())
                    errors.append((f"Error processing gear {er_gear.name}", e))

            if rmw_updates:
                try:
                    # Upload updates to RMW Hub
                    response = await self.rmw_client.upload_data(rmw_updates)
                    if response.status_code == 200:
                        success_times.extend([datetime.now()] * len(rmw_updates))
                        logger.info(f"Successfully uploaded {len(rmw_updates)} updates to RMW Hub")
                        await log_action_activity(
                            integration_id=self.integration_uuid,
                            action_log_type="upload",
                            level=LogLevel.INFO,
                            log=f"Successfully uploaded {len(rmw_updates)} updates to RMW Hub",
                            parent_log_id=upload_task_id,
                        )
                    else:
                        error_times.extend([datetime.now()] * len(rmw_updates))
                        errors.append(("Upload failed", f"Status: {response.status_code}, Content: {response.content}"))
                        logger.error(f"Upload failed with status {response.status_code}")
                except Exception as e:
                    error_times.extend([datetime.now()] * len(rmw_updates))
                    errors.append(("Upload error", e))
                    logger.error(f"Upload error: {e}")

            return processed_times, success_times, error_times, errors

        except Exception as e:
            logger.error(f"Error in upload task: {e}")
            await log_action_activity(
                integration_id=self.integration_uuid,
                action_log_type="upload",
                level=LogLevel.ERROR,
                log=f"Error in upload task: {e}",
                parent_log_id=upload_task_id,
            )
            return [], [], [datetime.now()], [("Upload task error", e)]

    async def _create_rmw_update_from_er_gear(
        self,
        er_gear: BuoyGear,
        display_id_to_gear_mapping: dict,
    ) -> Optional[GearSet]:
        """
        Create an RMW update from an EarthRanger gear.
        """
        # Extract deployment information from the gear
        additional_data = er_gear.additional or {}
        rmw_set_id = additional_data.get("rmwhub_set_id")
        
        if not rmw_set_id:
            logger.warning(f"Gear {er_gear.name} missing rmwhub_set_id, skipping")
            return None

        # Get the most recent location and timestamp
        last_location = er_gear.location
        last_updated = er_gear.last_updated

        # Create traps from devices
        traps = []
        for i, device in enumerate(er_gear.devices):
            device_location = device.location if hasattr(device, 'location') else last_location
            
            # Get deploy datetime and convert to string if needed
            deploy_datetime = getattr(device, 'last_deployed', last_updated)
            if hasattr(deploy_datetime, 'isoformat'):
                deploy_datetime = deploy_datetime.isoformat()
            elif not isinstance(deploy_datetime, str):
                deploy_datetime = str(deploy_datetime)
                
            # Get last_updated and convert to string if needed
            last_updated_str = last_updated
            if hasattr(last_updated_str, 'isoformat'):
                last_updated_str = last_updated_str.isoformat()
            elif not isinstance(last_updated_str, str):
                last_updated_str = str(last_updated_str)
            
            trap = Trap(
                id=device.device_id if hasattr(device, 'device_id') else f"device_{i}",
                sequence=1 if getattr(device, 'label', '') == "a" else 2,
                latitude=device_location.latitude if hasattr(device_location, 'latitude') else last_location.get("latitude", 0.0),
                longitude=device_location.longitude if hasattr(device_location, 'longitude') else last_location.get("longitude", 0.0),
                deploy_datetime_utc=deploy_datetime,
                surface_datetime_utc=None,
                retrieved_datetime_utc=None,
                status="deployed" if er_gear.is_active else "retrieved",
                accuracy="high",
                release_type="",
                is_on_end=getattr(device, 'label', '') == "a",
            )
            traps.append(trap)

        # Create the gear set
        gear_set = GearSet(
            vessel_id=additional_data.get("vessel_id", "unknown"),
            id=rmw_set_id,
            deployment_type=additional_data.get("deployment_type", "unknown"),
            traps_in_set=len(traps),
            trawl_path="",
            share_with=[],
            traps=traps,
            when_updated_utc=last_updated_str,
        )

        return gear_set

    async def create_display_id_to_gear_mapping(self, er_gears: List[BuoyGear]) -> dict:
        """
        Create a mapping of display IDs to gears for quick lookup.
        """
        mapping = {}
        for gear in er_gears:
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
