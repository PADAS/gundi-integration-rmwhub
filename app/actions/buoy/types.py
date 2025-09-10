from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel

class Environment(Enum):
    DEV = "Buoy Dev"
    STAGE = "Buoy Staging"
    PRODUCTION = "Buoy Prod"

class DeviceLocation(BaseModel):
    latitude: float
    longitude: float


class BuoyDevice(BaseModel):
    device_id: str
    label: str
    location: DeviceLocation
    last_updated: datetime
    last_deployed: Optional[datetime]


class BuoyGear(BaseModel):
    id: UUID
    display_id: str
    name: str
    status: str
    last_updated: datetime
    devices: List[BuoyDevice]
    type: str
    manufacturer: str
    location: Optional[DeviceLocation] = None
    additional: Optional[Dict[str, Any]] = None

    def create_haul_observation(self, recorded_at: datetime) -> List[Dict[str, Any]]:
        """
        Create an observation record for hauling the buoy gear.
        """
        from app.actions.rmwhub.types import SOURCE_TYPE, SUBJECT_SUBTYPE

        return [
            {
                "subject_name": self.display_id,
                "manufacturer_id": device.device_id,
                "subject_is_active": False,
                "source_type": SOURCE_TYPE,
                "subject_subtype": SUBJECT_SUBTYPE,
                "location": {
                    "lat": device.location.latitude,
                    "lon": device.location.longitude,
                },
                "recorded_at": recorded_at,
            }
            for device in self.devices
        ]
