import hashlib
from typing import List, Optional, Tuple

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
from pydantic import BaseModel, NoneStr, validator
from erclient import ERClient
from app.services.activity_logger import log_action_activity

logger = logging.getLogger(__name__)


SOURCE_TYPE = "ropeless_buoy"
SUBJECT_SUBTYPE = "ropeless_buoy_device"
GEAR_DEPLOYED_EVENT = "gear_deployed"
GEAR_RETRIEVED_EVENT = "gear_retrieved"
EPOCH = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc).isoformat()

class Trap(BaseModel):
    id: str
    sequence: int
    latitude: float
    longitude: float
    deploy_datetime_utc: Optional[NoneStr]
    surface_datetime_utc: Optional[NoneStr]
    retrieved_datetime_utc: Optional[NoneStr]
    status: str
    accuracy: str
    release_type: Optional[NoneStr]
    is_on_end: bool

    def __getitem__(self, key):
        return getattr(self, key)

    def get(self, key):
        return self.__getitem__(key)

    def __hash__(self):
        return hash(
            (
                self.id,
                self.sequence,
                self.latitude,
                self.longitude,
                self.deploy_datetime_utc,
            )
        )

    def get_latest_update_time(self) -> datetime:
        """
        Get the last updated time of the trap based on the status.
        """

        traptime = None
        if self.status == "deployed":
            traptime = self.deploy_datetime_utc or datetime.now()
        elif self.status == "retrieved":
            traptime = self.retrieved_datetime_utc or self.surface_datetime_utc or self.deploy_datetime_utc or datetime.now()
        return Trap.convert_to_utc(traptime)

    @classmethod
    def convert_to_utc(self, datetime_str: str) -> datetime:
        """
        Convert the datetime string to UTC.
        """
        naive_datetime_obj = parse_date(datetime_str)
        utc_datetime_obj = naive_datetime_obj.replace(tzinfo=timezone.utc)
        if not utc_datetime_obj:
            raise ValueError(f"Unable to parse datetime string: {datetime_str}")

        return utc_datetime_obj

class GearSet(BaseModel):
    vessel_id: str
    id: str
    deployment_type: str
    traps_in_set: Optional[int]
    trawl_path: str
    share_with: Optional[List[str]]
    traps: List[Trap]
    when_updated_utc: str

    @validator("trawl_path", pre=True)
    def none_to_empty(cls, v: object) -> object:
        if v is None:
            return ""
        return v

    @validator("share_with", pre=True)
    def none_to_empty_list(cls, v: object) -> object:
        if v is None:
            return []
        return v

    def __getitem__(self, key):
        return getattr(self, key)

    def get(self, key):
        return self.__getitem__(key)

    def __hash__(self):
        return hash((self.id, self.deployment_type, tuple(self.traps)))

    def get_devices(self) -> List:
        """
        Get the devices info for the gear set.
        """

        devices = []
        for trap in self.traps:
            devices.append(
                {
                    "device_id": "rmwhub_"
                    + trap.id.removeprefix("e_")
                    .removeprefix("rmwhub_")
                    .removeprefix("device_")
                    .removeprefix("edgetech_"),
                    "label": "a" if trap.sequence == 1 else "b",
                    "location": {
                        "latitude": trap.latitude,
                        "longitude": trap.longitude,
                    },
                    "last_deployed": self.when_updated_utc,
                    "last_updated": self.when_updated_utc,
                }
            )

        return devices

    async def create_observations(self) -> List:
        """
        Create observations for the gear set.
        """
        observations = []
        for trap in self.traps:

            display_id_hash = hashlib.sha256(str(self.id).encode()).hexdigest()[:12]
            subject_name = "rmwhub_" + trap.id
            
            observation = {
                "name": subject_name,
                "source": subject_name,
                "type": SOURCE_TYPE,
                "subject_type": SUBJECT_SUBTYPE,
                "is_active": True if trap.status == "deployed" else False,
                "recorded_at": self.when_updated_utc,
                "location": {"lat": trap.latitude, "lon": trap.longitude},
                "additional": {
                    "subject_is_active": True if trap.status == "deployed" else False,
                    "subject_name": subject_name,
                    "rmwhub_set_id": self.id,
                    "display_id": display_id_hash,
                    "event_type": GEAR_DEPLOYED_EVENT
                    if trap.status == "deployed"
                    else GEAR_RETRIEVED_EVENT,
                    "devices": self.get_devices(),
                },
            }


            observations.append(observation)

        return observations

    async def get_trap_ids(self) -> set:
        """
        Get the trap IDs for the gear set.
        """

        return {
            geartrap.id.replace("e_", "").replace("rmwhub_", "")
            for geartrap in self.traps
        }

    async def is_visited(self, visited: set) -> bool:
        """
        Check if the gearset has been visited.
        """

        traps_in_gearset = await self.get_trap_ids()
        return traps_in_gearset & visited


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
        self.er_subject_name_to_subject_mapping = {}
        self.options = kwargs.get("options", {})

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

        return self.convert_to_sets(response.json())

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
        sorted_traps_ids = sorted(traps_ids)
        cleaned_traps_ids = [
            RmwHubAdapter.clean_id_str(trap_id) for trap_id in sorted_traps_ids
        ]
        "".join(cleaned_traps_ids)
        return "".join(cleaned_traps_ids)

    async def generate_display_id_from_devices(self, devices):
        concat_devices = self._create_traps_gearsets_mapping_key(
            [device.get("device_id") for device in devices]
        )
        display_id_hash = hashlib.sha256(str(concat_devices).encode()).hexdigest()[:12]

        return display_id_hash

    async def _generate_trap_list(self, devices):
        """
        Generates a list of rmwHub trap IDs from a list of devices referenced in ER for the sake of matching
        """
        return sorted(
            [
                self.validate_id_length("e_" + self.clean_id_str(device["device_id"]))
                for device in devices
            ]
        )

    # Trap IDs must be atleast 32 characters and no more than 38 characters
    def validate_id_length(self, id_str: str):
        return id_str.ljust(32, "#")

    def get_er_subjects(self, start_datetime):
        return list(self.er_client._get(path = "subjects", params = {
            "include_inactive": True,
            "include_details": True,
            "updated_since": start_datetime,
            "subject_subtypes": "ropeless_buoy_device"
        }))

    async def process_upload(self, start_datetime: datetime) -> Tuple[List, dict]:
        """
        Process the sets from the Buoy API and upload to RMWHub.
        Returns a list of new observations for Earthranger with the new RmwHub set IDs.
        """

        logger.info("Processing updates to RMW Hub from ER...")

        # Normalize the extracted data into a list of updates following to the RMWHub schema:
        updates = []
        existing_updates = []

        # Get updates from the last interval_minutes in ER
        er_subjects = self.get_er_subjects(start_datetime)

        if er_subjects:
            await log_action_activity(
                integration_id=self.integration_id,
                action_id="pull_observations",
                title=f"Processing {len(er_subjects)} updated since {start_datetime.isoformat()} from ER.",
                level=LogLevel.INFO,
            )
    
        else:
            await log_action_activity(
                integration_id=self.integration_id,
                action_id="pull_observations",
                title="No subjects with new observations found in ER.",
                level=LogLevel.INFO,
            )
            return 0, {}

        # Iterate through er_subjects and determine what is an insert and what is an update to RmwHub
        # Based on the display ID existence on the RMW side
        failed_subjects = 0
        for subject in er_subjects:
            subject_name = subject.get("name")

            if subject_name.startswith("rmw"):
                logger.debug(
                    f"Subject ID {subject_name} originally from rmwHub. Skipped."
                )
                continue
            elif not subject_name:
                logger.error(f"Subject ID {subject['id']} has no name. No action.")
                continue

            devices = subject['additional'].get("devices", [])
            if not devices:
                logging.info(
                    f"No devices in latest observation for subject {subject['id']}.  Skipping..."
                )
                continue

            trap_list = await self._generate_trap_list(devices)
            if trap_list in existing_updates:
                continue

            try:
                rmwhub_set = await self._get_newest_set_from_rmwhub(trap_list)
            except httpx.ReadTimeout as e:
                logger.error(
                    f"Error reading from RMW Hub while getting newest set for Subject {subject_name}: {e}"
                )
                failed_subjects += 1
                continue
            except httpx.ConnectTimeout as e:
                logger.error(
                    f"Error connecting to RMW Hub while getting newest set for Subject {subject_name}: {e}"
                )
                failed_subjects += 1
                continue

            if rmwhub_set and (
                parse_date(rmwhub_set.when_updated_utc)
                > (parse_date(subject['updated_at']) + timedelta(minutes = 15))
            ):
                continue

            new_gearset = await self._create_rmw_update_from_er_subject(subject, rmwhub_set)
            if new_gearset:
                logger.info(f"Generated update for gearset {trap_list}")
                updates.append(new_gearset)
                existing_updates.append(trap_list)

        if not updates:
            logger.info("No updates to upload to RMW Hub API.")
            return 0, {"trap_count": 0}
        logger.info(f"Sending {len(updates)} updates to RMW Hub.")

        response = await self._upload_data(updates)
        num_new_observations = len(
            [trap.id for gearset in updates for trap in gearset.traps]
        )

        if failed_subjects:
            await log_action_activity(
                integration_id=self.integration_id,
                action_id="pull_observations",
                title=f"Number of failed ER subject uploads: {failed_subjects}",
                level=LogLevel.ERROR,
            )

        return num_new_observations, response

    async def _upload_data(
        self,
        updates: List[GearSet],
    ) -> dict:
        """
        Upload data to the RMWHub API using the RMWHubClient.

        Return RMWHub response if upload is successful, empty dict otherwise
        """

        response = await self.rmw_client.upload_data(updates)

        if response.status_code == 200:
            logger.info("Upload to RMW Hub API was successful.")
            result = json.loads(response.content)
            if len(result["result"]):
                logger.info(
                    f"Number of traps uploaded: {result['result']['trap_count']}"
                )
                logger.info(
                    f"Number of failed sets: {len(result['result']['failed_sets'])}"
                )
                return result["result"]

            logger.error(f"No info returned from RMW Hub API.")
            return {}
        else:
            logger.error(
                f"Failed to upload data to RMW Hub API. Error: {response.status_code} - {response.text}"
            )

        return {}

    async def _create_rmw_update_from_er_subject(
        self,
        er_subject: dict,
        rmw_gearset: GearSet = None,
    ) -> Optional[GearSet]:
        """
        Create new updates from ER data for upload to RMWHub.

        :param er_subject: ER subject to create updates from
        :param rmw_gearset: RMW gear set to update (not required for new inserts)
        """

        deployed = er_subject.get('is_active')
        devices = er_subject.get("additional", {}).get("devices")
        trap_id_mapping = ({RmwHubAdapter.clean_id_str(trap.id): trap for trap in rmw_gearset.traps} if rmw_gearset
            else {}
        )

        traps = []
        for device in devices:
            
            subject_name = er_subject.get("name")
            device_name = device.get("device_id")
            cleaned_id = RmwHubAdapter.clean_id_str(device_name)
            trap_id = (cleaned_id if rmw_gearset and subject_name.startswith("rmw")
                else "e_" + cleaned_id)

            if not deployed and not rmw_gearset:
                logger.debug(f"This trap ({trap_id}) is not being deployed and still does not exist in RMW Hub, skipping.")
                continue

            rmw_trap_datetime = device.get("last_updated", er_subject['updated_at'])
            rmw_trap_datetime = (self.convert_datetime_to_utc(rmw_trap_datetime) if rmw_trap_datetime
                else None
            )
            # deploy_datetime_utc is required, so in retrieve events, we will use the current deployed datetime
            current_deployed_datetime = (
                trap_id_mapping.get(
                    RmwHubAdapter.clean_id_str(trap_id)
                ).deploy_datetime_utc
                if not deployed
                else None
            )
            traps.append(
                Trap(
                    id=self.validate_id_length(trap_id),
                    sequence=1 if device.get("label") == "a" else 2,
                    latitude=device.get("location").get("latitude"),
                    longitude=device.get("location").get("longitude"),
                    deploy_datetime_utc=rmw_trap_datetime
                    if deployed
                    else current_deployed_datetime,
                    surface_datetime_utc=rmw_trap_datetime if deployed else None,
                    retrieved_datetime_utc=None if deployed else rmw_trap_datetime,
                    status="deployed" if deployed else "retrieved",
                    accuracy="",
                    is_on_end=True,
                )
            )

        # No traps found for the gear set it will be skipped
        if len(traps) == 0:
            return None

        # Create gear set:
        if not rmw_gearset:
            set_id = "e_" + str(uuid.uuid4())
            vessel_id = ""
        else:
            set_id = rmw_gearset.id
            vessel_id = rmw_gearset.vessel_id

        share_with = self.options.get("share_with", [])
        gear_set = GearSet(
            vessel_id=vessel_id,
            id=set_id,
            deployment_type="trawl" if len(traps) > 1 else "single",
            traps_in_set=len(traps),
            trawl_path="",
            share_with=share_with,
            traps=traps,
            when_updated_utc=datetime.now(timezone.utc).isoformat(),
        )

        return gear_set

    async def create_set_id_to_gearset_mapping(self, sets: List[GearSet]) -> dict:
        """
        Create a mapping of Set IDs to GearSets.
        """

        set_id_to_set_mapping = {}
        for gear_set in sets:
            set_id_to_set_mapping[RmwHubAdapter.clean_id_str(gear_set.id)] = gear_set
        return set_id_to_set_mapping

    async def create_name_to_subject_mapping(self, er_subjects: List) -> dict:
        """
        Create a mapping of ER subject names to subjects.
        """

        name_to_subject_mapping = {}
        for subject in er_subjects:
            if subject.get("name"):
                name_to_subject_mapping[
                    RmwHubAdapter.clean_id_str(subject.get("name"))
                ] = subject
            else:
                msg = "Cannot clean string. Subject name is empty."
                await log_action_activity(
                    integration_id=self.integration_id,
                    action_id="pull_observations",
                    title=msg,
                    level=LogLevel.ERROR,
                )
        return name_to_subject_mapping

    @classmethod
    def clean_id_str(cls, subject_name: str):
        """
        Resolve the ID string to just the UUID
        """
        if not subject_name:
            msg = "Cannot clean string. Subject name is empty."
            logger.error(msg)
            return None

        cleaned_str = (
            subject_name.removeprefix("device_")
            .removeprefix("rmwhub_")
            .removeprefix("rmw_")
            .removeprefix("e_")
            .removeprefix("edgetech_")
            .rstrip("#")
            .lower()
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


class RmwHubClient:
    HEADERS = {"accept": "application/json", "Content-Type": "application/json"}

    def __init__(self, api_key: str, rmw_url: str):
        self.api_key = api_key
        self.rmw_url = rmw_url

    async def search_hub(self, start_datetime: str, status: bool = None) -> dict:
        """
        Downloads data from the RMWHub API using the search_hub endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        data = {
            "format_version": 0.1,
            "api_key": self.api_key,
            "max_sets": 10000,
            # "status": "deployed", // Pull all data not just deployed gear
            "start_datetime_utc": start_datetime.astimezone(pytz.utc).isoformat(),
        }

        if status:
            data["status"] = status

        url = self.rmw_url + "/search_hub/"

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=RmwHubClient.HEADERS, json=data)

        if response.status_code != 200:
            logger.error(
                f"Failed to download data from RMW Hub API. Error: {response.status_code} - {response.text}"
            )

        return response.text

    async def upload_data(self, updates: List[GearSet]) -> httpx.Response:
        """
        Upload data to the RMWHub API using the upload_data endpoint.
        ref: https://ropeless.network/api/docs
        """

        url = self.rmw_url + "/upload_deployments/"
        sets = [jsonable_encoder(update) for update in updates]

        for set_entry in sets:
            set_entry["set_id"] = set_entry.pop("id")
            for trap in set_entry["traps"]:
                trap["trap_id"] = trap.pop("id")
                trap["release_type"] = trap.get("release_type") or ""

        upload_data = {"format_version": 0, "api_key": self.api_key, "sets": sets}

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url, headers=RmwHubClient.HEADERS, json=upload_data
            )

        if response.status_code != 200:
            logger.error(
                f"Failed to upload data to RMW Hub API. Error: {response.status_code} - {response.content}"
            )

        return response
