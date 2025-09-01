from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl


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
    is_active: bool = True
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


class LastPositionStatus(BaseModel):
    last_voice_call_start_at: Optional[datetime]
    radio_state_at: Optional[datetime]
    radio_state: str


class Geometry(BaseModel):
    type: str
    coordinates: List[float]


class CoordinateProperties(BaseModel):
    time: datetime


class FeatureProperties(BaseModel):
    title: str
    subject_type: str
    subject_subtype: str
    id: UUID
    stroke: str
    stroke_opacity: float = Field(..., alias="stroke-opacity")
    stroke_width: int = Field(..., alias="stroke-width")
    image: str
    last_voice_call_start_at: Optional[datetime]
    location_requested_at: Optional[datetime]
    radio_state_at: datetime
    radio_state: str
    coordinateProperties: CoordinateProperties
    DateTime: datetime


class Feature(BaseModel):
    type: str
    geometry: Geometry
    properties: FeatureProperties


class ObservationSubject(BaseModel):
    content_type: str
    id: UUID
    name: str
    subject_type: str
    subject_subtype: str
    common_name: Optional[str]
    additional: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    is_active: bool
    user: Optional[Any]
    tracks_available: bool
    image_url: str
    last_position_status: Optional[LastPositionStatus]
    last_position_date: Optional[datetime]
    last_position: Optional[Feature]
    device_status_properties: Optional[Any]
    url: HttpUrl

    @property
    def location(self) -> tuple[float, float]:
        """
        Return the last known location as a tuple of (latitude, longitude).
        """
        return (self.latitude, self.longitude)

    @property
    def latitude(self) -> float:
        """
        Return the latitude of the last known location.
        """
        if not self.last_position or not self.last_position.geometry:
            raise ValueError("Last position is not available.")
        return self.last_position.geometry.coordinates[1]

    @property
    def longitude(self) -> float:
        """
        Return the longitude of the last known location.
        """
        if not self.last_position or not self.last_position.geometry:
            raise ValueError("Last position is not available.")
        return self.last_position.geometry.coordinates[0]

    def create_observation(
        self, recorded_at: Optional[datetime], is_active: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        Create observations based on the subject's last position and status.
        Returns an observation record.
        """
        from app.actions.rmwhub.types import GEAR_DEPLOYED_EVENT, GEAR_RETRIEVED_EVENT

        if not self.last_position or not self.last_position.geometry:
            raise ValueError("Last position is not available.")

        devices = self.additional.get("devices", [])
        if not devices:
            raise ValueError("No devices available in additional information.")

        is_active = is_active if is_active is not None else self.is_active
        observation = {
            "name": self.name,
            "source": self.name,
            "type": self.subject_type,
            "subject_type": self.subject_subtype,
            "recorded_at": recorded_at.isoformat()
            or datetime.now().isoformat(),
            "location": {"lat": self.latitude, "lon": self.longitude},
            "additional": {
                "subject_name": self.name,
                "rmwhub_set_id": self.additional.get("rmwhub_set_id"),
                "display_id": self.additional.get("display_id"),
                "subject_is_active": is_active,
                "event_type": (
                    GEAR_DEPLOYED_EVENT if is_active else GEAR_RETRIEVED_EVENT
                ),
                "devices": self.additional.get("devices", []),
            },
        }

        return observation
