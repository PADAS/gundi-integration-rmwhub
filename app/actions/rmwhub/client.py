import asyncio
import json
import logging
from typing import List

import httpx
from datetime import timezone

from fastapi.encoders import jsonable_encoder

from .types import GearSet

logger = logging.getLogger(__name__)

# Retry configuration for transient RMW API failures (502, 503, 504) on upload
UPLOAD_RETRY_COUNT = 3
UPLOAD_RETRY_DELAY_SEC = 5
UPLOAD_RETRYABLE_STATUS_CODES = (502, 503, 504)


class RmwHubClient:
    """Client for communicating with the RMW Hub API."""

    HEADERS = {"accept": "application/json", "Content-Type": "application/json"}

    def __init__(
        self,
        api_key: str,
        rmw_url: str,
        default_timeout: float = 60.0,
        connect_timeout: float = 10.0,
        read_timeout: float = 60.0,
    ):
        self.api_key = api_key
        # Normalize base URL: no trailing slash so path concatenation never produces "//"
        self.rmw_url = rmw_url.rstrip("/") if rmw_url else rmw_url
        self.default_timeout = httpx.Timeout(
            timeout=default_timeout,
            connect=connect_timeout,
            read=read_timeout
        )

    async def search_hub(self, start_datetime: str) -> dict:
        """
        Downloads data from the RMWHub API using the search_hub endpoint.
        ref: https://ropeless.network/api/docs#/Download
        """

        data = {
            "format_version": 0.1,
            "api_key": self.api_key,
            "max_sets": 10000,
            "start_datetime_utc": start_datetime.astimezone(timezone.utc).isoformat(),  # Pull all data from the start date
        }

        url = self.rmw_url + "/search_hub/"

        async with httpx.AsyncClient(timeout=self.default_timeout) as client:
            response = await client.post(url, headers=RmwHubClient.HEADERS, json=data)

        if response.status_code != 200:
            logger.error(
                f"Failed to download data from RMW Hub API. Error: {response.status_code} - {response.text}"
            )

        return response.text

    async def upload_data(self, updates: List[GearSet]) -> httpx.Response:
        """
        Upload data to the RMWHub API using the upload_data endpoint.
        Retries on 502/503/504 (transient gateway/server errors).
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

        set_ids = [s.get("set_id", "unknown") for s in sets]
        logger.info("Uploading %d gear sets to RMW Hub API at %s (set_ids=%s)", len(sets), url, set_ids)
        logger.debug("Upload payload: %d sets, set_ids=%s", len(sets), set_ids)

        async with httpx.AsyncClient(timeout=self.default_timeout) as client:
            last_response: httpx.Response | None = None
            for attempt in range(1, UPLOAD_RETRY_COUNT + 1):
                response = await client.post(
                    url, headers=RmwHubClient.HEADERS, json=upload_data
                )
                last_response = response
                if response.status_code == 200:
                    return response
                if response.status_code not in UPLOAD_RETRYABLE_STATUS_CODES:
                    logger.error(
                        "Failed to upload data to RMW Hub API. Error: %s - %s",
                        response.status_code,
                        response.content,
                    )
                    return response
                if attempt < UPLOAD_RETRY_COUNT:
                    logger.warning(
                        "RMW Hub upload got %s (attempt %d/%d), retrying in %ds...",
                        response.status_code,
                        attempt,
                        UPLOAD_RETRY_COUNT,
                        UPLOAD_RETRY_DELAY_SEC,
                    )
                    await asyncio.sleep(UPLOAD_RETRY_DELAY_SEC)
                else:
                    logger.error(
                        "Failed to upload data to RMW Hub API after %d attempts. Last error: %s - %s",
                        UPLOAD_RETRY_COUNT,
                        response.status_code,
                        response.content,
                    )
            return last_response
