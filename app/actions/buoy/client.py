import logging
from typing import List, Optional, Dict, Any, AsyncIterator
from datetime import datetime
from urllib.parse import urljoin, urlparse

import httpx
from .types import BuoyGear, BuoyDevice, DeviceLocation

logger = logging.getLogger(__name__)

class BuoyClient:
    """Client for interacting with EarthRanger Gear API."""
    
    def __init__(
        self, 
        er_token: str, 
        er_site: str,
        default_timeout: float = 30.0,
        connect_timeout: float = 5.0,
        read_timeout: float = 30.0,
    ):
        self.er_token = er_token
        self.er_site = self._sanitize_base_url(er_site)
        self.default_timeout = httpx.Timeout(
            timeout=default_timeout,
            connect=connect_timeout,
            read=read_timeout
        )
        self.headers = {
            "Authorization": f"Bearer {self.er_token}",
            "Content-Type": "application/json",
        }

    def _sanitize_base_url(self, url: str) -> str:
        """
        Sanitize the base URL to ensure proper format.
        
        Args:
            url: Base URL to sanitize
            
        Returns:
            Properly formatted base URL
        """
        if not url:
            raise ValueError("Base URL cannot be empty")
        
        # Add https:// if no scheme is provided
        if not url.startswith(('http://', 'https://')):
            url = f"https://{url}"
        
        # Ensure URL ends with a slash
        if not url.endswith('/'):
            url += '/'
        
        # Validate the URL structure
        parsed = urlparse(url)
        if not parsed.netloc:
            raise ValueError(f"Invalid URL format: {url}")
        
        return url

    @staticmethod
    def create_timeout(
        timeout: float = 30.0,
        connect: float = 5.0,
        read: float = 30.0,
        write: float = 10.0,
        pool: float = 5.0,
    ) -> httpx.Timeout:
        """
        Create a custom timeout configuration.
        
        Args:
            timeout: Total timeout for the entire request
            connect: Timeout for establishing a connection
            read: Timeout for reading data from the server
            write: Timeout for writing data to the server
            pool: Timeout for acquiring a connection from the pool
            
        Returns:
            httpx.Timeout object with the specified settings
        """
        return httpx.Timeout(
            timeout=timeout,
            connect=connect,
            read=read,
            write=write,
            pool=pool,
        )

    async def iter_gears(
        self,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[httpx.Timeout] = None,
    ) -> AsyncIterator[BuoyGear]:
        """
        Iterate over gears from EarthRanger API using async generator.
        
        This method yields gears one by one without loading all pages into memory,
        making it more memory-efficient for large datasets.
        
        Args:
            params: Optional query parameters
            timeout: Optional timeout settings (overrides defaults)
            
        Yields:
            BuoyGear objects one at a time
        """
        url = urljoin(self.er_site, "gear/")
        
        # Use provided timeout or fall back to default
        client_timeout = timeout or self.default_timeout
        
        async with httpx.AsyncClient(timeout=client_timeout) as client:
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
                
                # Yield each gear individually
                for item in results:
                    yield self._parse_gear(item)

                url = page_data.get("next")
                # Clear params for subsequent requests (they're already in the next URL)
                params = None

    async def get_all_gears(
        self,
        timeout: Optional[httpx.Timeout] = None,
    ) -> List[BuoyGear]:
        """
        Retrieve all gears from EarthRanger API with pagination handling.
        
        Args:
            params: Optional query parameters
            timeout: Optional timeout settings (overrides defaults)
            
        Returns:
            List of BuoyGear objects
        """
        gears = []
        async for gear in self.iter_gears(params={"state": "deployed"}, timeout=timeout):
            gears.append(gear)
        async for gear in self.iter_gears(params={"state": "hauled"}, timeout=timeout):
            gears.append(gear)
        return gears

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
                source_id=device_data.get("source_id", ""),
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
