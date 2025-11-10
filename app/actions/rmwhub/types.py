import logging
from typing import List, Optional

from pydantic import BaseModel, NoneStr, validator

logger = logging.getLogger(__name__)


class Trap(BaseModel):
    id: str
    sequence: int
    latitude: float
    longitude: float
    manufacturer: Optional[NoneStr]
    serial_number: Optional[NoneStr]
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


class GearSet(BaseModel):
    vessel_id: str
    id: str
    deployment_type: str
    traps_in_set: Optional[int]
    trawl_path: Optional[dict]
    share_with: Optional[List[str]]
    traps: List[Trap]
    when_updated_utc: str

    @validator("trawl_path", pre=True)
    def none_to_empty(cls, v: object) -> object:
        if v is None:
            return {}
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
