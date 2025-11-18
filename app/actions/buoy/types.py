from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel


class Environment(Enum):
    DEV = "Buoy Dev"
    STAGE = "Buoy Staging"
    PRODUCTION = "Buoy Prod"
    RF_1086 = "Buoy RF 1086 Dev"

class DeviceLocation(BaseModel):
    latitude: float
    longitude: float


class BuoyDevice(BaseModel):
    device_id: str
    mfr_device_id: str
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

        return [
            {
                "source_name": self.display_id,
                "source": device.device_id,
                "type": SOURCE_TYPE,
                "subject_type": SUBJECT_SUBTYPE,
                "location": {
                    "lat": device.location.latitude,
                    "lon": device.location.longitude,
                },
                "recorded_at": recorded_at,
            }
            for device in self.devices
        ]
