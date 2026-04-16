import asyncio
import json
import logging
from typing import Dict, List, Optional

import httpx
from dateutil import parser as dateutil_parser
from datetime import datetime, timezone

from fastapi.encoders import jsonable_encoder

from .types import GearSet

logger = logging.getLogger(__name__)

# Retry configuration for transient RMW API failures (502, 503, 504)
RETRY_COUNT = 3
RETRY_DELAY_SEC = 5
RETRYABLE_STATUS_CODES = (502, 503, 504)

# Pagination configuration for search_hub
SEARCH_PAGE_SIZE = 1000
MAX_SEARCH_PAGES = 20


class RmwHubClient:
    """Client for communicating with the RMW Hub API."""

    HEADERS = {"accept": "application/json", "Content-Type": "application/json"}

    def __init__(
        self,
        api_key: str,
        rmw_url: str,
        default_timeout: float = 120.0,
        connect_timeout: float = 10.0,
        read_timeout: float = 120.0,
        upload_timeout: float = 300.0,
        upload_connect_timeout: float = 10.0,
        upload_read_timeout: float = 300.0,
    ):
        self.api_key = api_key
        # Normalize base URL: no trailing slash so path concatenation never produces "//"
        self.rmw_url = rmw_url.rstrip("/") if rmw_url else rmw_url
        self.default_timeout = httpx.Timeout(
            timeout=default_timeout,
            connect=connect_timeout,
            read=read_timeout
        )
        self.upload_timeout = httpx.Timeout(
            timeout=upload_timeout,
            connect=upload_connect_timeout,
            read=upload_read_timeout,
        )

    async def search_hub(self, start_datetime: datetime) -> str:
        """
        Downloads data from the RMWHub API using the search_hub endpoint.
        Retries on 502/503/504 (transient gateway/server errors).
        ref: https://ropeless.network/api/docs#/Download
        """

        data = {
            "format_version": 0.1,
            "api_key": self.api_key,
            "max_sets": SEARCH_PAGE_SIZE,
            "start_datetime_utc": start_datetime.astimezone(timezone.utc).isoformat(),  # Pull all data from the start date
        }

        url = self.rmw_url + "/search_hub/"

        async with httpx.AsyncClient(timeout=self.default_timeout) as client:
            last_response: httpx.Response | None = None
            for attempt in range(1, RETRY_COUNT + 1):
                try:
                    response = await client.post(url, headers=RmwHubClient.HEADERS, json=data)
                except httpx.TimeoutException as e:
                    logger.error(
                        "RMW Hub API error | POST /search_hub/ | %s: request timed out (timeout=%s)",
                        type(e).__name__, self.default_timeout.read,
                    )
                    if attempt < RETRY_COUNT:
                        logger.warning("Retrying (attempt %d/%d) in %ds...", attempt, RETRY_COUNT, RETRY_DELAY_SEC)
                        await asyncio.sleep(RETRY_DELAY_SEC)
                        continue
                    raise
                except httpx.HTTPError as e:
                    logger.error(
                        "RMW Hub API error | POST /search_hub/ | %s: %s",
                        type(e).__name__, e,
                    )
                    if attempt < RETRY_COUNT:
                        logger.warning("Retrying (attempt %d/%d) in %ds...", attempt, RETRY_COUNT, RETRY_DELAY_SEC)
                        await asyncio.sleep(RETRY_DELAY_SEC)
                        continue
                    raise

                last_response = response
                if response.status_code == 200:
                    return response.text
                if response.status_code not in RETRYABLE_STATUS_CODES:
                    logger.error(
                        "RMW Hub API error | POST /search_hub/ | HTTP %s: %s",
                        response.status_code,
                        response.text[:500],
                    )
                    return response.text
                if attempt < RETRY_COUNT:
                    logger.warning(
                        "RMW Hub API error | POST /search_hub/ | HTTP %s (attempt %d/%d), retrying in %ds...",
                        response.status_code,
                        attempt,
                        RETRY_COUNT,
                        RETRY_DELAY_SEC,
                    )
                    await asyncio.sleep(RETRY_DELAY_SEC)
                else:
                    logger.error(
                        "RMW Hub API error | POST /search_hub/ | HTTP %s after %d attempts: %s",
                        response.status_code,
                        RETRY_COUNT,
                        response.text[:500],
                    )
            return last_response.text

    async def search_hub_all(self, start_datetime: datetime) -> Dict:
        """
        Downloads all data from the RMWHub API, paginating through results.

        Makes repeated calls to search_hub with max_sets=SEARCH_PAGE_SIZE,
        advancing start_datetime_utc to the latest when_updated_utc in each
        page until fewer than SEARCH_PAGE_SIZE sets are returned.

        Returns a dict with ``{"format_version": 0.1, "sets": [...]}``.
        """
        all_sets: List[dict] = []
        seen_set_ids: set = set()
        current_start = start_datetime
        pages_fetched = 0

        for page in range(1, MAX_SEARCH_PAGES + 1):
            logger.info(
                "Fetching page %d from RMW Hub (start_datetime=%s)",
                page,
                current_start.isoformat(),
            )
            response_text = await self.search_hub(current_start)

            try:
                response_json = json.loads(response_text)
            except json.JSONDecodeError:
                logger.error("Invalid JSON response from RMW Hub on page %d", page)
                break

            sets = response_json.get("sets", [])
            if not sets:
                break

            new_count = 0
            for s in sets:
                set_id = s.get("set_id", "")
                if set_id not in seen_set_ids:
                    seen_set_ids.add(set_id)
                    all_sets.append(s)
                    new_count += 1

            pages_fetched = page
            logger.info(
                "Page %d: received %d sets (%d new, %d total)",
                page,
                len(sets),
                new_count,
                len(all_sets),
            )

            # If we got fewer than a full page, there's nothing left
            if len(sets) < SEARCH_PAGE_SIZE:
                break

            # Detect pagination stall: every set on this page was already seen
            if new_count == 0:
                logger.warning(
                    "No new sets on page %d — pagination cursor has not advanced, stopping",
                    page,
                )
                break

            # Advance start_datetime to the max when_updated_utc in this page.
            # Parse each timestamp to a proper datetime before comparing, since
            # string comparison can pick the wrong value when formats differ
            # (e.g. "Z" vs "+00:00").
            parsed_times = []
            for s in sets:
                raw = s.get("when_updated_utc", "")
                if not raw:
                    continue
                try:
                    dt = dateutil_parser.isoparse(raw)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    parsed_times.append(dt)
                except (ValueError, TypeError):
                    logger.error(
                        "Could not parse when_updated_utc for pagination: %s",
                        raw,
                    )

            if not parsed_times:
                logger.error("No valid when_updated_utc found on page %d, stopping pagination", page)
                break

            next_start = max(parsed_times)
            if next_start <= current_start:
                logger.warning(
                    "Pagination cursor did not advance (stuck at %s), stopping",
                    current_start.isoformat(),
                )
                break
            current_start = next_start

        if pages_fetched >= MAX_SEARCH_PAGES:
            logger.warning(
                "Reached MAX_SEARCH_PAGES (%d) — results may be incomplete. "
                "Fetched %d sets so far; consider increasing MAX_SEARCH_PAGES or "
                "narrowing the start_datetime window.",
                MAX_SEARCH_PAGES,
                len(all_sets),
            )
        logger.info(
            "Fetched %d total sets from RMW Hub across %d page(s)",
            len(all_sets),
            pages_fetched,
        )
        return {"format_version": 0.1, "sets": all_sets}

    async def download_and_convert(
        self, start_datetime: datetime
    ) -> List[GearSet]:
        """
        Download gear sets from RMW Hub and convert to GearSet objects.

        This is a convenience method for callers that need the parsed data
        without going through an RmwHubAdapter (e.g. downloading once and
        sharing the result across multiple destinations).
        """
        from .adapter import RmwHubAdapter

        response_json = await self.search_hub_all(start_datetime)
        return RmwHubAdapter.convert_to_sets(response_json)

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

        try:
            async with httpx.AsyncClient(timeout=self.upload_timeout) as client:
                last_response: httpx.Response | None = None
                for attempt in range(1, RETRY_COUNT + 1):
                    try:
                        response = await client.post(
                            url, headers=RmwHubClient.HEADERS, json=upload_data
                        )
                    except httpx.TimeoutException as e:
                        logger.error(
                            "RMW Hub API error | POST /upload_deployments/ | %s: request timed out (timeout=%s, set_ids=%s)",
                            type(e).__name__, self.upload_timeout.read, set_ids,
                        )
                        if attempt < RETRY_COUNT:
                            logger.warning("Retrying (attempt %d/%d) in %ds...", attempt, RETRY_COUNT, RETRY_DELAY_SEC)
                            await asyncio.sleep(RETRY_DELAY_SEC)
                            continue
                        raise
                    except httpx.HTTPError as e:
                        logger.error(
                            "RMW Hub API error | POST /upload_deployments/ | %s: %s (set_ids=%s)",
                            type(e).__name__, e, set_ids,
                        )
                        if attempt < RETRY_COUNT:
                            logger.warning("Retrying (attempt %d/%d) in %ds...", attempt, RETRY_COUNT, RETRY_DELAY_SEC)
                            await asyncio.sleep(RETRY_DELAY_SEC)
                            continue
                        raise

                    last_response = response
                    if response.status_code == 200:
                        return response
                    if response.status_code not in RETRYABLE_STATUS_CODES:
                        logger.error(
                            "RMW Hub API error | POST /upload_deployments/ | HTTP %s: %s (set_ids=%s)",
                            response.status_code,
                            response.text[:500],
                            set_ids,
                        )
                        return response
                    if attempt < RETRY_COUNT:
                        logger.warning(
                            "RMW Hub API error | POST /upload_deployments/ | HTTP %s (attempt %d/%d), retrying in %ds...",
                            response.status_code,
                            attempt,
                            RETRY_COUNT,
                            RETRY_DELAY_SEC,
                        )
                        await asyncio.sleep(RETRY_DELAY_SEC)
                    else:
                        logger.error(
                            "RMW Hub API error | POST /upload_deployments/ | HTTP %s after %d attempts: %s (set_ids=%s)",
                            response.status_code,
                            RETRY_COUNT,
                            response.text[:500],
                            set_ids,
                        )
                return last_response
        except Exception as e:
            logger.error(
                "RMW Hub API error | POST /upload_deployments/ | %s: %s (set_ids=%s)",
                type(e).__name__, e, set_ids,
            )
            raise
