import logging
from datetime import datetime, timezone
from typing import List, Optional

from dateparser import parse as parse_date
from pydantic import BaseModel, NoneStr, validator

from app.actions.buoy.types import GEAR_DEPLOYED_EVENT, GEAR_RETRIEVED_EVENT, SOURCE_TYPE, SUBJECT_SUBTYPE

logger = logging.getLogger(__name__)

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
    trawl_path: Optional[str]
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


    async def build_observations(self) -> List:
        """
        Build observations payload for the gear set as a single gear entity.
        """
        if not self.traps:
            return []

        observations = []
        for trap in self.traps:
            recorded_at = trap.get_latest_update_time()

            if trap.status == "deployed":
                event_type = GEAR_DEPLOYED_EVENT
            elif trap.status == "retrieved":
                event_type = GEAR_RETRIEVED_EVENT
            else:
                raise ValueError(f"Unknown trap status: {trap.status}")
            
            observation = {
                "source_name": self.id,
                "source": trap.id,
                "location": {
                    "lat": trap.latitude,
                    "lon": trap.longitude
                },
                "recorded_at": recorded_at,
                "type": SOURCE_TYPE,
                "subject_type": SUBJECT_SUBTYPE,
                "additional": {
                    "event_type": event_type,
                    "raw": self.dict()
                }
            }

            observations.append(observation)

        return observations