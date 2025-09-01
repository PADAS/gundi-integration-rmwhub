import hashlib
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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

    def _create_observation_record(
        self,
        subject_name: str,
        source_name: str,
        lat: float,
        lon: float,
        is_active: bool,
        recorded_at: str,
    ) -> Dict[str, Any]:
        """Return an observation record with the given parameters."""
        raw = self.dict()
        return {
            "subject_name": subject_name,
            "subject_type": SUBJECT_SUBTYPE,
            "recorded_at": recorded_at,
            "source_type": SOURCE_TYPE,
            "manufacturer_id": source_name,
            "is_active": is_active,
            "location": {"lat": lat, "lon": lon},
            "additional": {
                "event_type": GEAR_DEPLOYED_EVENT if is_active else GEAR_RETRIEVED_EVENT,
            },
            "source_additional": {"raw": raw},
        }

    def _build_observations(
        self,
        is_active: bool,
        recorded_at: str,
    ) -> List[Dict[str, Any]]:
        """
        Return one or more observations for deployment or retrieval events.
        Creates one observation per trap in the gearset.
        """
        observations = []
        
        # Generate unique subject_name for this gearset
        subject_name = str(uuid.uuid4())
        
        for trap in self.traps:
            # Create manufacturer_id based on trap info
            source_name = f"rmwhub_{trap.id}_{self.vessel_id}"
            
            # Determine if this specific trap is active
            trap_is_active = trap.status == "deployed" if is_active else False
            
            observation = self._create_observation_record(
                subject_name=subject_name,
                source_name=source_name,
                lat=trap.latitude,
                lon=trap.longitude,
                is_active=trap_is_active,
                recorded_at=recorded_at,
            )
            observations.append(observation)

        return observations

    async def create_observations(self) -> List:
        """
        Create observations for the gear set following EdgeTech pattern.
        Creates one observation per trap instead of one per gearset.
        """
        if not self.traps:
            return []
        
        # Determine overall status based on all traps
        deployed_count = sum(1 for trap in self.traps if trap.status == "deployed")
        is_active = deployed_count > 0
        
        recorded_at = self.when_updated_utc
        
        observations = self._build_observations(
            is_active=is_active,
            recorded_at=recorded_at,
        )

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
