import hashlib
import logging
from datetime import datetime, timezone
from typing import List, Optional

from dateparser import parse as parse_date
from pydantic import BaseModel, NoneStr, validator

logger = logging.getLogger(__name__)

SOURCE_TYPE = "ropeless_buoy"
SUBJECT_SUBTYPE = "ropeless_buoy_gearset"
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
    def convert_to_utc(cls, datetime_str: str) -> datetime:
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
                    "device_id": f"rmwhub_{trap.id}",
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
        Create observations for the gear set as a single gear entity.
        """
        if not self.traps:
            return []
            
        # Use the first trap's location as the primary location
        primary_trap = self.traps[0]
        
        display_id_hash = hashlib.sha256(str(self.id).encode()).hexdigest()[:12]
        subject_name = f"rmwhub_{display_id_hash}"
        
        # Determine overall status based on all traps
        deployed_count = sum(1 for trap in self.traps if trap.status == "deployed")
        is_active = deployed_count > 0
        
        observation = {
            "name": subject_name,
            "source": subject_name,
            "type": SOURCE_TYPE,
            "subject_type": SUBJECT_SUBTYPE,
            "is_active": is_active,
            "recorded_at": self.when_updated_utc,
            "location": {"lat": primary_trap.latitude, "lon": primary_trap.longitude},
            "additional": {
                "subject_is_active": is_active,
                "subject_name": subject_name,
                "rmwhub_set_id": self.id,
                "display_id": display_id_hash,
                "event_type": GEAR_DEPLOYED_EVENT if is_active else GEAR_RETRIEVED_EVENT,
                "devices": self.get_devices(),
                "deployment_type": self.deployment_type,
                "traps_in_set": len(self.traps),
                "vessel_id": self.vessel_id,
            },
        }

        return [observation]

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
