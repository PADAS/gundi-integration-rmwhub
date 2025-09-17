import json
import logging
from typing import List

import httpx
import pytz
from fastapi.encoders import jsonable_encoder

from .types import GearSet

logger = logging.getLogger(__name__)


class RmwHubClient:
    """Client for communicating with the RMW Hub API."""
    
    HEADERS = {"accept": "application/json", "Content-Type": "application/json"}

    def __init__(self, api_key: str, rmw_url: str):
        self.api_key = api_key
        self.rmw_url = rmw_url

    async def search_hub(self, start_datetime: str) -> dict:
        """
        Downloads data from the RMWHub API using the search_hub endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        data = {
            "format_version": 0.1,
            "api_key": self.api_key,
            "max_sets": 10000,
            "start_datetime_utc": start_datetime.astimezone(pytz.utc).isoformat(),  # Pull all data from the start date
        }

        url = "https://ropeless.network/api" + "/search_hub/"

        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=RmwHubClient.HEADERS, json=data)

        if response.status_code != 200:
            logger.error(
                f"Failed to download data from RMW Hub API. Error: {response.status_code} - {response.text}"
            )

        return response.text

    async def upload_data(self, updates: List[GearSet]) -> httpx.Response:
        """
        Upload data to the RMWHub API using the upload_data endpoint.
        ref: https://ropeless.network/api/docs
        """

        url = self.rmw_url + "/upload_deployments/"
        sets = [jsonable_encoder(update) for update in updates]

        for set_entry in sets:
            set_entry["set_id"] = set_entry.pop("id")
            for trap in set_entry["traps"]:
                trap["trap_id"] = trap.pop("id")
                trap["release_type"] = trap.get("release_type") or ""

        upload_data = {"format_version": 0, "api_key": self.api_key, "sets": sets}

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url, headers=RmwHubClient.HEADERS, json=upload_data
            )

        if response.status_code != 200:
            logger.error(
                f"Failed to upload data to RMW Hub API. Error: {response.status_code} - {response.content}"
            )

        return response
