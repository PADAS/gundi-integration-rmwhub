from typing import List, Optional, Tuple, AsyncIterator, Dict, Any

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
from .types import GearSet, Trap

logger = logging.getLogger(__name__)

# Manufacturer string used to identify RMW Hub gears. Put in a constant so
# we don't duplicate the literal throughout the file.
RMWHUB_MANUFACTURER = "rmwhub"


def is_valid_uuid(uuid_string):
    try:
        uuid.UUID(str(uuid_string))
        return True
    except ValueError:
        return False

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
        self, start_datetime: str, status: str = "all"
    ) -> List[GearSet]:
        """
        Downloads data from the RMW Hub API using the search_hub endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        response = await self.rmw_client.search_hub(start_datetime)

        try:
            response_json = json.loads(response)
        except json.JSONDecodeError:
            logger.error(f"Failed to download data from RMW Hub API. Invalid JSON response: {response}")
            return []

        if "sets" not in response_json:
            logger.error(f"Failed to download data from RMW Hub API. Error: {response}")
            return []

        rmwsets = self.convert_to_sets(response_json)

        if status == "deployed":
            rmwsets = [s for s in rmwsets if any(t.status == "deployed" for t in s.traps)]
            for s in rmwsets:
                s.traps = [t for t in s.traps if t.status == "deployed"]

        if status == "hauled":
            rmwsets = [s for s in rmwsets if all(t.status == "hauled" for t in s.traps)]
        
        return rmwsets

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

    async def process_download(self, rmw_sets: List[GearSet]) -> List[Dict[str, Any]]:
        """
        Process the sets from the RMW Hub API and convert them to gear payloads.
        Returns a list of gear payloads ready to be sent to the Buoy API.
        """
        gears = await self.gear_client.get_all_gears()

        trap_id_to_gear_mapping = {
            device.mfr_device_id: gear
            for gear in gears
            for device in gear.devices
        }

        gear_payloads = []
        skipped_retrieved_traps_missing_in_er = []
        matched_status_traps = []
        
        for gearset in rmw_sets:
            # Group traps by their deployment/haul status
            traps_to_deploy = []
            traps_to_haul = []
            if not is_valid_uuid(gearset.id) or any(not is_valid_uuid(trap.id) for trap in gearset.traps):
                logger.warning(f"Skipping gearset {gearset.id} due to invalid UUIDs.")
                continue
            
            for trap in gearset.traps:
                er_gear = trap_id_to_gear_mapping.get(trap.id)

                if er_gear and er_gear.display_id != gearset.id:
                    continue  # That trap is deployed in multiple gears/sets, and that's not the correct one

                if not er_gear and trap.status == "retrieved":
                    skipped_retrieved_traps_missing_in_er.append(trap.id)
                    continue
                
                if er_gear:
                    if (trap.status == "deployed" and er_gear.status == "deployed") or \
                       (trap.status == "retrieved" and er_gear.status == "hauled"):
                        matched_status_traps.append(trap.id)
                        continue
                
                # Separate traps by status
                if trap.status == "deployed":
                    traps_to_deploy.append(trap)
                elif trap.status == "retrieved":
                    traps_to_haul.append(trap)
            
            # Create gear payloads for deployment
            if traps_to_deploy:
                payload = self._create_gear_payload_from_gearset(
                    gearset,
                    traps_to_deploy,
                    device_status="deployed"
                )
                gear_payloads.append(payload)
            
            # Create gear payloads for hauling
            if traps_to_haul:
                payload = self._create_gear_payload_from_gearset(
                    gearset,
                    traps_to_haul,
                    device_status="hauled"
                )
                gear_payloads.append(payload)
        
        logger.info(f"Skipped {len(skipped_retrieved_traps_missing_in_er)} retrieved traps missing in EarthRanger: {skipped_retrieved_traps_missing_in_er}")
        logger.info(f"Skipped matching {len(matched_status_traps)} traps with same status in EarthRanger: {matched_status_traps}")
        logger.info(f"Created {len(gear_payloads)} gear payloads to send to Buoy API")
        
        return gear_payloads

    def _create_gear_payload_from_gearset(
        self,
        gearset: GearSet,
        traps: List[Trap],
        device_status: str
    ) -> Dict[str, Any]:
        """
        Create a gear payload from a RMW Hub gearset in the format expected by the Buoy API.
        
        Args:
            gearset: The RMW Hub gear set
            traps: List of traps to include in this payload
            device_status: Status of the devices (deployed/hauled)
        
        Returns:
            Dict in the format expected by /api/v1.0/gear/ POST endpoint
        """
        devices = []

        for trap in traps:
            # Get the appropriate timestamp based on status
            if device_status == "deployed":
                last_deployed = trap.deploy_datetime_utc or datetime.now().isoformat()
                last_updated = last_deployed
            else:  # hauled
                last_deployed = trap.deploy_datetime_utc or datetime.now().isoformat()
                last_updated = trap.retrieved_datetime_utc or trap.surface_datetime_utc or last_deployed

            device = {
                "device_id": trap.id,
                "last_deployed": last_deployed,
                "last_updated": last_updated,
                "device_status": device_status,
                "location": {
                    "latitude": trap.latitude,
                    "longitude": trap.longitude,
                },
                "device_additional_data": json.loads(json.dumps(trap.json(), default=str))
            }
            if trap.release_type and trap.release_type != "none":
                device["release_type"] = trap.release_type 
            devices.append(device)
        
        # Determine deployment type
        deployment_type = gearset.deployment_type or ("trawl" if len(devices) > 1 else "single")
        
        # Build payload
        payload = {
            "deployment_type": deployment_type,
            "set_id": gearset.id,
            "devices_in_set": len(devices),
            "devices": devices,
        }
        
        # Add initial_deployment_date only for new deployments
        if device_status == "deployed" and traps:
            # Use the earliest deployment time from the traps
            deployment_times = [trap.deploy_datetime_utc for trap in traps if trap.deploy_datetime_utc]
            if deployment_times:
                earliest_deployment = min(deployment_times)
            else:
                earliest_deployment = None
            if earliest_deployment:
                payload["initial_deployment_date"] = earliest_deployment
        
        return payload

    async def send_gear_to_buoy_api(self, gear_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send gear payload to the Buoy API POST endpoint.

        Args:
            gear_payload: The gear payload in the format expected by /api/v1.0/gear/

        Returns:
            Dict containing the API response
        """
        return await self.gear_client.send_gear_to_buoy_api(gear_payload)


    async def iter_er_gears(self, start_datetime: datetime = None, state: str = None) -> AsyncIterator[BuoyGear]:
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
        params['page_size'] = 1000
        if state:
            params['state'] = state
        
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
            action_id="pull_observations",
            level=LogLevel.INFO,
            title="Starting upload task",
        )

        try:
            # Use streaming approach for better memory efficiency
            rmw_updates = []
            errors = []
            gear_count = 0

            # Stream gears and process them one by one
            async for er_gear in self.iter_er_gears(start_datetime=start_datetime, state="hauled"):
                if er_gear.manufacturer.lower() == RMWHUB_MANUFACTURER:
                    continue  # Skip RMW Hub gears to avoid uploading their own data
                gear_count += 1
                try:
                    rmw_update = await self._create_rmw_update_from_er_gear(er_gear)
                    if rmw_update:
                        rmw_updates.append(rmw_update)
                        logger.info(f"Processed gear {er_gear.name}")
                except Exception as e:
                    logger.error(f"Error processing gear {er_gear.name}: {e}")
                    errors.append((f"Error processing gear {er_gear.name}", e))
            
            async for er_gear in self.iter_er_gears(start_datetime=start_datetime, state="deployed"):
                gear_count += 1
                try:
                    rmw_update = await self._create_rmw_update_from_er_gear(er_gear)
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
                    action_id="pull_observations",
                    level=LogLevel.INFO,
                    title="No gear found in EarthRanger, skipping upload.",
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
                                action_id="pull_observations",
                                level=LogLevel.WARNING,
                                title=f"Failed to upload {len(failed_sets)} sets: {failed_sets}",
                                data={"failed_sets": failed_sets},
                            )
                        
                        logger.info(f"Successfully uploaded {trap_count} traps to RMW Hub")
                        await log_action_activity(
                            integration_id=self.integration_uuid,
                            action_id="pull_observations",
                            level=LogLevel.INFO,
                            title=f"Successfully uploaded {trap_count} traps to RMW Hub",
                            data={"trap_count": trap_count},
                        )

                        return trap_count, response_data
                    else:
                        logger.error(f"Upload failed with status {response.status_code}")
                        await log_action_activity(
                            integration_id=self.integration_uuid,
                            action_id="pull_observations",
                            level=LogLevel.ERROR,
                            title=f"Upload failed with status {response.status_code}",
                            data={"upload_task_id": upload_task_id, "rmw_response": response.text, "rmw_sets": rmw_updates},
                        )
                        return 0, {}
                except Exception as e:
                    logger.error(f"Upload error: {e}")
                    await log_action_activity(
                        integration_id=self.integration_uuid,
                        action_id="pull_observations",
                        level=LogLevel.ERROR,
                        title=f"Upload error: {e}",
                        data={"upload_task_id": upload_task_id},
                    )
                    return 0, {}

        except Exception as e:
            logger.error(f"Error in upload task: {e}")
            await log_action_activity(
                integration_id=self.integration_uuid,
                action_id="pull_observations",
                level=LogLevel.ERROR,
                title=f"Error in upload task: {e}",
                data={"upload_task_id": upload_task_id},
            )
            return 0, []

    def _get_serial_number_from_device_id(self, device_id: str, manufacturer: str) -> str:
        """
        Convert device_id to a serial number format acceptable by RMW Hub API.
        Maximum length is 32 characters.
        """
        if manufacturer.lower() == "edgetech":
            return device_id.split("_")[0]
        try:
            uuid_obj = uuid.UUID(device_id)
            # Return hex format (32 characters, no dashes)
            return uuid_obj.hex
        except ValueError:
            pass
        # If not a UUID, truncate to 32 characters if needed
        return device_id[:32] if len(device_id) > 32 else device_id

    async def _create_rmw_update_from_er_gear(
        self,
        er_gear: BuoyGear,
    ) -> Optional[GearSet]:
        """
        Create an RMW update from an EarthRanger gear.
        """
        logger.info('Creating RMW update from EarthRanger gear: %s', er_gear.name)
        logger.info('Raw gear data: %s', json.dumps(er_gear.dict(), default=str))
        if er_gear.manufacturer.lower() == RMWHUB_MANUFACTURER:
            return None  # Skip RMW Hub gears to avoid uploading their own data
        traps = []
        for i, device in enumerate(er_gear.devices):
            if not device.last_deployed:
                logger.info(f"Skipping device {device.device_id} in gear {er_gear.name} due to missing last_deployed")
                continue
            traps.append(
                Trap(
                    id=str(device.device_id),
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
                    serial_number=self._get_serial_number_from_device_id(device.mfr_device_id, er_gear.manufacturer)
                )
            )
        if not traps:
            return None
        gear_set = GearSet(
            vessel_id="",
            id=str(er_gear.id),
            deployment_type="trawl" if len(er_gear.devices) > 1 else "single",
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
            if gear.manufacturer.lower() == RMWHUB_MANUFACTURER:
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
