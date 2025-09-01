import json
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

import httpx
from .types import BuoyGear, BuoyDevice, DeviceLocation

logger = logging.getLogger(__name__)


class BuoyClient:
    """Client for interacting with EarthRanger Gear API."""
    
    def __init__(self, er_token: str, er_site: str):
        self.er_token = er_token
        self.er_site = er_site
        self.headers = {
            "Authorization": f"Bearer {self.er_token}",
            "Content-Type": "application/json",
        }

    async def get_er_gears(
        self,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[BuoyGear]:
        """
        Get gears from EarthRanger API.
        
        Args:
            params: Optional query parameters
            
        Returns:
            List of BuoyGear objects
        """
        url = f"{self.er_site}gear/"
        items = []

        async with httpx.AsyncClient() as client:
            while url:
                response = await client.get(url, headers=self.headers, params=params)
                
                if response.status_code != 200:
                    logger.error(
                        f"Failed to get gears. Status code: {response.status_code} Body: {response.text}"
                    )
                    break

                data = response.json()

                if "data" not in data:
                    logger.error("Unexpected response structure")
                    break

                page_data = data["data"]

                if "results" not in page_data:
                    logger.error("No results field in response")
                    break

                results = page_data["results"]
                items.extend(results)

                url = page_data.get("next")
                # Clear params for subsequent requests (they're already in the next URL)
                params = None

        if len(items) == 0:
            logger.warning("No gears found")

        return [self._parse_gear(item) for item in items]

    async def create_gear(self, gear_data: Dict[str, Any]) -> Optional[BuoyGear]:
        """
        Create a new gear in EarthRanger.
        
        Args:
            gear_data: Gear data to create
            
        Returns:
            Created BuoyGear object or None if failed
        """
        url = f"{self.er_site}gear/"
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url, 
                headers=self.headers, 
                json=gear_data
            )
            
            if response.status_code not in [200, 201]:
                logger.error(
                    f"Failed to create gear. Status code: {response.status_code} Body: {response.text}"
                )
                return None
            
            return self._parse_gear(response.json())

    async def update_gear(self, gear_id: str, gear_data: Dict[str, Any]) -> Optional[BuoyGear]:
        """
        Update an existing gear in EarthRanger.
        
        Args:
            gear_id: ID of the gear to update
            gear_data: Updated gear data
            
        Returns:
            Updated BuoyGear object or None if failed
        """
        url = f"{self.er_site}gear/{gear_id}/"
        
        async with httpx.AsyncClient() as client:
            response = await client.patch(
                url, 
                headers=self.headers, 
                json=gear_data
            )
            
            if response.status_code != 200:
                logger.error(
                    f"Failed to update gear {gear_id}. Status code: {response.status_code} Body: {response.text}"
                )
                return None
            
            return self._parse_gear(response.json())

    def _parse_gear(self, data: Dict[str, Any]) -> BuoyGear:
        """
        Parse gear data from API response into BuoyGear object.
        
        Args:
            data: Raw gear data from API
            
        Returns:
            BuoyGear object
        """
        devices = []
        for device_data in data.get("devices", []):
            location = DeviceLocation(
                latitude=device_data.get("location", {}).get("latitude", 0.0),
                longitude=device_data.get("location", {}).get("longitude", 0.0)
            )
            
            device = BuoyDevice(
                device_id=device_data.get("device_id", ""),
                label=device_data.get("label", ""),
                location=location,
                last_updated=datetime.fromisoformat(device_data.get("last_updated", datetime.now().isoformat())),
                last_deployed=datetime.fromisoformat(device_data.get("last_deployed")) if device_data.get("last_deployed") else None
            )
            devices.append(device)
        
        return BuoyGear(
            id=data.get("id", ""),
            display_id=data.get("display_id", ""),
            name=data.get("name", data.get("display_id", "")),  # Use name or fallback to display_id
            status=data.get("status", ""),
            last_updated=datetime.fromisoformat(data.get("last_updated", datetime.now().isoformat())),
            devices=devices,
            type=data.get("type", ""),
            manufacturer=data.get("manufacturer", "")
        )
