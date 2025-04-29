from datetime import datetime, timezone
import hashlib
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

    def create_rmwhub_gearset_observation(
        trap_id: str = "test_trap_id_001",
        set_id: str = "test_set_id_001",
        vessel_id: str = "test_vessel_id_001",
        event_type: str = "gear_retrieved",
        recorded_at: str = datetime.now(timezone.utc).isoformat(),
        location: Dict[str, float] = {"lat": -5.19816, "lon": 122.8113},
        display_id: str = "test_display_hash_001",
        devices: List[Dict] = None,
    ) -> Dict:
        """
        Factory function to create a gearset object for testing.
        """
        if devices is None:
            devices = [
                {
                    "label": "a",
                    "location": {
                        "latitude": str(location["lat"]),
                        "longitude": str(location["lon"]),
                    },
                    "device_id": trap_id,
                    "last_updated": recorded_at,
                },
                {
                    "label": "b",
                    "location": {
                        "latitude": "44.63648713",
                        "longitude": "-63.58044069",
                    },
                    "device_id": "test_trap_id_1",
                    "last_updated": recorded_at,
                },
            ]

        return {
            "name": trap_id,
            "source": f"rmwhub_{trap_id}",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": recorded_at,
            "location": location,
            "additional": {
                "subject_name": trap_id,
                "rmwHub_set_id": set_id,
                "vessel_id": vessel_id,
                "display_id": display_id,
                "event_type": event_type,
                "devices": devices,
            },
        }

    def create_gearset_observation_from_gearset(gearset: GearSet) -> Dict:
        """
        Factory function to create a gearset object for testing based on a GearSet instance.
        """
        # Extract the first trap for the main details
        first_trap = gearset.traps[0] if gearset.traps else None

        if not first_trap:
            raise ValueError("GearSet must contain at least one trap.")

        # Generate devices from traps
        devices = [
            {
                "label": "a" if trap.sequence == 1 else "b",
                "location": {
                    "latitude": str(trap.latitude),
                    "longitude": str(trap.longitude),
                },
                "device_id": trap.id,
                "last_updated": trap.get_latest_update_time().isoformat(),
            }
            for trap in gearset.traps
        ]

        # Create the gearset object
        gearset_object = {
            "name": first_trap.id,
            "source": f"rmwhub_{first_trap.id}",
            "type": "ropeless_buoy",
            "subject_type": "ropeless_buoy_device",
            "recorded_at": first_trap.get_latest_update_time().isoformat(),
            "location": {"lat": first_trap.latitude, "lon": first_trap.longitude},
            "additional": {
                "subject_name": first_trap.id,
                "rmwHub_set_id": gearset.id,
                "vessel_id": gearset.vessel_id,
                "display_id": hashlib.sha256(gearset.id.encode()).hexdigest()[:12],
                "event_type": "gear_deployed"
                if first_trap.status == "deployed"
                else "gear_retrieved",
                "devices": devices,
            },
        }

        return gearset_object


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

    def create_latest_observation(
        id: str = "test_subject_id_001",
    ) -> List:
        """
        Factory method to create a Subject dictionary with the latest observation.
        """
        return [
            {
                "id": id,
                "location": {"latitude": 40.0, "longitude": -70.0},
                "created_at": "2025-01-28T14:51:02.996570-08:00",
                "recorded_at": "2025-01-30T00:00:00Z",
                "source": "random-string",
                "exclusion_flags": 0,
                "observation_details": {
                    "devices": [
                        {
                            "label": "a",
                            "location": {
                                "latitude": 40.0,
                                "longitude": 70.0,
                            },
                            "device_id": "test_subject_name_"
                            + str(random.randint(100, 999)),
                            "last_updated": "2025-01-25T13:22:32+00:00",
                        }
                    ],
                    "display_id": "84f360b0a8a5",
                    "event_type": "gear_deployed",
                    "subject_is_active": True,
                },
            }
        ]

    def create_mock_subject_from_observation(observation: Dict) -> Dict:
        """
        Create a mock subject based on an observation.
        """
        first_device = observation["additional"]["devices"][0]

        return {
            "content_type": "observations.subject",
            "id": "0302a774-1971-4a64-8264-1d7f17969442",
            "name": observation["name"],
            "subject_type": "ropeless_buoy",
            "subject_subtype": "ropeless_buoy_device",
            "common_name": None,
            "additional": {
                "devices": [
                    {
                        "label": first_device["label"],
                        "location": {
                            "latitude": float(first_device["location"]["latitude"]),
                            "longitude": float(first_device["location"]["longitude"]),
                        },
                        "device_id": first_device["device_id"],
                        "last_updated": first_device["last_updated"],
                    }
                ],
                "display_id": observation["additional"]["display_id"],
                "event_type": observation["additional"]["event_type"],
                "subject_name": observation["additional"]["subject_name"],
            },
            "created_at": "2025-01-28T14:51:02.996570-08:00",
            "updated_at": "2025-01-28T14:51:02.996595-08:00",
            "is_active": True,
            "user": None,
            "tracks_available": False,
            "image_url": "/static/pin-black.svg",
            "last_position_date": "2025-01-16T17:33:21+00:00",
            "last_position": {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        float(first_device["location"]["longitude"]),
                        float(first_device["location"]["latitude"]),
                    ],
                },
                "properties": {
                    "title": observation["name"],
                    "subject_type": observation["subject_type"],
                    "subject_subtype": "ropeless_buoy_device",
                    "id": "0006a86a-9a99-4112-94b7-f72190ff178f",
                    "stroke": "#FFFF00",
                    "stroke-opacity": 1.0,
                    "stroke-width": 2,
                    "image": "https://buoy.dev.pamdas.org/static/pin-black.svg",
                    "radio_state_at": "1970-01-01T00:00:00+00:00",
                    "radio_state": "na",
                    "coordinateProperties": {"time": "2025-01-16T17:33:21+00:00"},
                    "DateTime": "2025-01-16T17:33:21+00:00",
                },
            },
            "url": f"https://buoy.dev.pamdas.org/api/v1.0/subject/{'0302a774-1971-4a64-8264-1d7f17969442'}",  # Example URL
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
