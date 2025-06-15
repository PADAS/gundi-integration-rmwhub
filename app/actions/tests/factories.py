from datetime import datetime, timezone
import random
from typing import Dict, List, Optional
from app.actions.rmwhub import GearSet, Trap
from datetime import datetime, timezone


class GearsetFactory:
    def create(
        traps_in_set: int,
        vessel_id: str = "vessel_001",
        set_id: str = "gearset_001",
        deployment_type: str = "trawl",
        trawl_path: str = "path_001",
        share_with: Optional[List[str]] = None,
        traps: Optional[List[Trap]] = None,
        when_updated_utc: str = datetime.now(timezone.utc).isoformat(),
    ) -> dict:
        """
        Factory function to create a gearset for unit testing.
        """
        if traps is None:
            traps = [
                Trap(
                    id=f"trap_{i}",
                    sequence=i,
                    latitude=0.0,
                    longitude=0.0,
                    deploy_datetime_utc=datetime.now(timezone.utc).isoformat(),
                    surface_datetime_utc=datetime.now(timezone.utc).isoformat(),
                    retrieved_datetime_utc=None,
                    status="deployed",
                    accuracy="high",
                    release_type="manual",
                    is_on_end=True,
                )
                for i in range(1, traps_in_set + 1)
            ]

        return GearSet(
            vessel_id=vessel_id,
            id=set_id,
            deployment_type=deployment_type,
            traps_in_set=traps_in_set,
            trawl_path=trawl_path,
            share_with=share_with or [],
            traps=traps,
            when_updated_utc=when_updated_utc,
        )


class SubjectFactory:
    def create(
        id: str = "test_subject_id_001",
        name: Optional[str] = None,
        latitude: float = 40.0,
        longitude: float = -70.0,
        last_updated: str = "2025-01-30T00:00:00Z",
        event_type: str = "gear_position_rmwhub",
        devices: Optional[List] = None,
    ) -> Dict:
        """
        Factory method to create a Subject dictionary.
        """
        if not name:
            name = "test_subject_name_001" + str(random.randint(100, 999))
        return {
            "content_type": "observations.subject",
            "id": id,
            "name": name,
            "subject_type": "ropeless_buoy",
            "subject_subtype": "ropeless_buoy_device",
            "common_name": None,
            "additional": {
                "devices": devices
                if devices
                else [
                    {
                        "label": "a",
                        "location": {"latitude": latitude, "longitude": longitude},
                        "device_id": name,
                        "last_updated": last_updated,
                    }
                ],
                "display_id": "30548f5def46",
                "event_type": event_type,
                "subject_name": name,
            },
            "created_at": "2025-01-28T14:51:02.996570-08:00",
            "updated_at": "2025-01-28T14:51:02.996570-08:00",
            "is_active": True,
            "user": None,
            "tracks_available": False,
            "image_url": "/static/pin-black.svg",
            "last_position_date": "2025-01-16T17:33:21+00:00",
            "last_position": {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [-70.443459307605, 41.83290438292462],
                },
                "properties": {
                    "title": "edgetech_88CE99D36A_A",
                    "subject_type": "ropeless_buoy",
                    "subject_subtype": "ropeless_buoy_device",
                    "id": "0006a86a-9a99-4112-94b7-f72190ff178f",
                    "stroke": "#FFFF00",
                    "stroke-opacity": 1.0,
                    "stroke-width": 2,
                    "image": f"https://buoy.dev.pamdas.org/static/pin-black.svg",
                    "radio_state_at": "1970-01-01T00:00:00+00:00",
                    "radio_state": "na",
                    "coordinateProperties": {"time": "2025-01-16T17:33:21+00:00"},
                    "DateTime": "2025-01-16T17:33:21+00:00",
                },
            },
            "url": f"https://buoy.dev.pamdas.org/api/v1.0/subject/{id}",
        }

class TrapFactory:
    def create(
        trap_id: str = "trap_001",
        sequence: int = 1,
        latitude: float = 0.0,
        longitude: float = 0.0,
        deploy_datetime_utc: str = datetime.now(timezone.utc).isoformat(),
        surface_datetime_utc: Optional[str] = None,
        retrieved_datetime_utc: Optional[str] = None,
        status: str = "deployed",
        accuracy: str = "high",
        release_type: str = "manual",
        is_on_end: bool = True,
    ) -> Trap:
        """
        Factory function to create a Trap object for unit testing.
        """
        return Trap(
            id=trap_id,
            sequence=sequence,
            latitude=latitude,
            longitude=longitude,
            deploy_datetime_utc=deploy_datetime_utc,
            surface_datetime_utc=surface_datetime_utc,
            retrieved_datetime_utc=retrieved_datetime_utc,
            status=status,
            accuracy=accuracy,
            release_type=release_type,
            is_on_end=is_on_end,
        )
