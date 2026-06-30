import copy
import time
from typing import Any, Dict, List, Optional, Set

import requests as req

from gcs import GCSClient
from config import ENVIRONMENT, SCHEDULER_INTERVAL_MINUTES

from .utils import json_hash


class SnapshotAPIClient:

    DEFAULT_ENDPOINT = "https://hub.snapshot.org/graphql"
    _PROPOSAL_FIELDS = (
        "id\n"
        "ipfs\n"
        "title\n"
        "body\n"
        "choices\n"
        "start\n"
        "end\n"
        "scores\n"
        "scores_total\n"
        "scores_state\n"
        "quorum\n"
        "state\n"
        "type\n"
        "author\n"
        "space { id }\n"
        "created\n"
        "updated\n"
        "plugins\n"
    )

    def __init__(self, endpoint: Optional[str] = None, http_client: Optional[Any] = None):
        self.endpoint = endpoint or self.DEFAULT_ENDPOINT
        self.http_client = http_client or req

    def fetch_space_proposals(self, space: str, batch_size: int = 1000) -> List[Dict[str, Any]]:
        proposals: List[Dict[str, Any]] = []
        skip = 0

        query = (
            "query Proposals($space: String!, $first: Int!, $skip: Int!) {"
            "  proposals("
            "    first: $first"
            "    skip: $skip"
            "    where: { space_in: [$space], flagged: false }"
            "    orderBy: \"created\""
            "    orderDirection: asc"
            "  ) {"
            f"    {self._PROPOSAL_FIELDS}"
            "  }"
            "}"
        )

        while True:
            payload = {
                "query": query,
                "variables": {"space": space, "first": batch_size, "skip": skip},
            }
            response = self.http_client.post(self.endpoint, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            if "errors" in data:
                raise RuntimeError(f"Snapshot API error: {data['errors']}")

            batch = data.get("data", {}).get("proposals", [])
            if not batch:
                break

            proposals.extend(batch)
            if len(batch) < batch_size:
                break
            skip += batch_size

        return proposals

    def fetch_proposal(self, proposal_id: str) -> Optional[Dict[str, Any]]:
        query = (
            "query Proposal($id: String!) {"
            "  proposal(id: $id) {"
            f"    {self._PROPOSAL_FIELDS}"
            "  }"
            "}"
        )
        payload = {"query": query, "variables": {"id": proposal_id}}
        response = self.http_client.post(self.endpoint, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        if "errors" in data:
            raise RuntimeError(f"Snapshot API error: {data['errors']}")
        return data.get("data", {}).get("proposal")


class SnapshotSync:

    # A proposal must be confirmed absent (missing from the space list AND null on
    # a direct id lookup) on this many consecutive syncs before it is retired, so a
    # transient API blip or indexer lag can't delete a live proposal.
    DELETION_MISS_THRESHOLD = 2

    def __init__(
        self,
        space_id: str,
        snapshot_space: Optional[str] = None,
        api_client: Optional[SnapshotAPIClient] = None,
        now_provider: Optional[Any] = None,
    ) -> None:
        self.space_id = space_id
        self.snapshot_space = snapshot_space or space_id
        self.api_client = api_client or SnapshotAPIClient()
        self.now_provider = now_provider or (lambda: int(time.time()))

    async def refresh_list(self, gcs_client: GCSClient) -> None:
        proposals = self.api_client.fetch_space_proposals(self.snapshot_space)
        prefix = f"data/{self.space_id}/proposal/snapshot/raw/"

        existing = await self._load_existing_proposals(gcs_client, prefix)
        seen_ids: Set[str] = set()

        for proposal in proposals:
            proposal_id = proposal["id"]
            seen_ids.add(proposal_id)

            blob_name = f"{prefix}{proposal_id}.json"
            existing_record = existing.get(proposal_id)
            proposal_copy = copy.deepcopy(proposal)

            liveness = self._proposal_liveness(proposal_copy.get("state"))
            timestamp = self.now_provider()

            existing_hash = None
            if existing_record:
                existing_hash = existing_record.get("data_eng_properties", {}).get("hash")

            proposal_hash = self._compute_payload_hash(proposal_copy, liveness)

            if existing_hash == proposal_hash:
                if existing_record is not None:
                    data_props = existing_record.setdefault("data_eng_properties", {})
                    data_props["synced_at"] = timestamp
                    existing[proposal_id] = existing_record
                continue

            data_eng_properties = {
                "liveness": liveness,
                "source": "snapshot",
                "synced_at": timestamp,
                "hash": proposal_hash,
            }

            proposal_copy["data_eng_properties"] = data_eng_properties

            metadata = {
                "proposal_id": proposal_id,
                "source": "snapshot",
                "liveness": liveness,
                "hash": proposal_hash,
            }

            cache_control = self._cache_control(liveness)

            await gcs_client.upload_dict(
                proposal_copy,
                blob_name,
                metadata=metadata,
                cache_control=cache_control,
            )
            existing[proposal_id] = proposal_copy

        await self._mark_deleted_proposals(gcs_client, existing, seen_ids, prefix)
        await self._write_summary(gcs_client, existing)

    async def _load_existing_proposals(
        self,
        gcs_client: GCSClient,
        prefix: str,
    ) -> Dict[str, Dict[str, Any]]:
        proposals: Dict[str, Dict[str, Any]] = {}
        blobs = await gcs_client.list_blobs(prefix=prefix)
        for blob in blobs:
            blob_name = self._as_json_name(blob.name)
            data = await gcs_client.read_dict(blob_name)
            if not data:
                continue
            proposal_id = data.get("id") or self._proposal_id_from_name(blob_name)
            proposals[proposal_id] = data
        return proposals

    async def _mark_deleted_proposals(
        self,
        gcs_client: GCSClient,
        existing: Dict[str, Dict[str, Any]],
        seen_ids: Set[str],
        prefix: str,
    ) -> None:
        timestamp = self.now_provider()
        for proposal_id, record in list(existing.items()):
            data_props = record.get("data_eng_properties", {})
            if data_props.get("liveness") == "deleted":
                continue

            present = proposal_id in seen_ids
            if not present:
                # The list query excludes flagged proposals (where flagged=false)
                # and can transiently drop live ones, so a direct id lookup is
                # required to confirm. Flagged or transiently-absent proposals
                # still resolve here and must NOT be treated as deleted.
                try:
                    present = self.api_client.fetch_proposal(proposal_id) is not None
                except Exception as exc:
                    # Never infer deletion from an API error.
                    print(f"snapshot: could not verify {proposal_id}, skipping: {exc}")
                    continue

            prior_strikes = data_props.get("missing_count", 0)

            if present:
                if prior_strikes:
                    await self._write_missing_count(
                        gcs_client, existing, proposal_id, record, prefix, 0, timestamp
                    )
                continue

            # Confirmed absent from both the list and a direct lookup. Require
            # consecutive misses before retiring so indexer lag / replica
            # inconsistency can't delete a live proposal.
            strikes = prior_strikes + 1
            if strikes < self.DELETION_MISS_THRESHOLD:
                await self._write_missing_count(
                    gcs_client, existing, proposal_id, record, prefix, strikes, timestamp
                )
                continue

            await self._retire_proposal(
                gcs_client, existing, proposal_id, record, prefix, strikes, timestamp
            )

    async def _write_missing_count(
        self,
        gcs_client: GCSClient,
        existing: Dict[str, Dict[str, Any]],
        proposal_id: str,
        record: Dict[str, Any],
        prefix: str,
        count: int,
        timestamp: int,
    ) -> None:
        # Strikes live only in data_eng_properties and leave the content hash
        # unchanged, so a bump/reset is not seen as a content change downstream.
        proposal_copy = copy.deepcopy(record)
        proposal_copy.setdefault("id", proposal_id)
        data_props = proposal_copy.setdefault("data_eng_properties", {})
        if count:
            data_props["missing_count"] = count
        else:
            data_props.pop("missing_count", None)
        data_props["synced_at"] = timestamp

        liveness = data_props.get("liveness", "live")
        proposal_hash = data_props.get("hash")
        if proposal_hash is None:
            proposal_hash = self._compute_payload_hash(proposal_copy, liveness)
            data_props["hash"] = proposal_hash

        metadata = {
            "proposal_id": proposal_id,
            "source": "snapshot",
            "liveness": liveness,
            "hash": proposal_hash,
        }
        await gcs_client.upload_dict(
            proposal_copy,
            f"{prefix}{proposal_id}.json",
            metadata=metadata,
            cache_control=self._cache_control(liveness),
        )
        existing[proposal_id] = proposal_copy

    async def _retire_proposal(
        self,
        gcs_client: GCSClient,
        existing: Dict[str, Dict[str, Any]],
        proposal_id: str,
        record: Dict[str, Any],
        prefix: str,
        strikes: int,
        timestamp: int,
    ) -> None:
        proposal_copy = copy.deepcopy(record)
        proposal_copy.setdefault("id", proposal_id)

        data_eng_properties = proposal_copy.setdefault("data_eng_properties", {})
        data_eng_properties.update(
            {
                "liveness": "deleted",
                "source": "snapshot",
                "deleted_at": timestamp,
                "synced_at": timestamp,
                "missing_count": strikes,
            }
        )

        proposal_hash = self._compute_payload_hash(
            proposal_copy,
            "deleted",
            extra={"deleted_at": timestamp},
        )
        data_eng_properties["hash"] = proposal_hash

        metadata = {
            "proposal_id": proposal_id,
            "source": "snapshot",
            "liveness": "deleted",
            "hash": proposal_hash,
        }
        await gcs_client.upload_dict(
            proposal_copy,
            f"{prefix}{proposal_id}.json",
            metadata=metadata,
            cache_control=self._cache_control("deleted"),
        )
        existing[proposal_id] = proposal_copy

    async def _write_summary(
        self,
        gcs_client: GCSClient,
        proposals: Dict[str, Dict[str, Any]],
    ) -> None:
        live: List[Dict[str, Any]] = []
        archived: List[Dict[str, Any]] = []

        for record in proposals.values():
            data_eng = record.get("data_eng_properties", {})
            liveness = data_eng.get("liveness", "live")
            if liveness == "deleted":
                continue

            summary = copy.deepcopy(record)
            summary.pop("body", None)
            if "data_eng_properties" in summary:
                summary["data_eng_properties"].pop("hash", None)

            target = live if liveness == "live" else archived
            target.append(summary)

        live.sort(key=lambda item: item.get("end") or 0, reverse=True)
        archived.sort(key=lambda item: item.get("end") or 0, reverse=True)

        base_path = f"data/{self.space_id}/proposal_list/snapshot"
        await gcs_client.upload_ndjson(live, f"{base_path}/live.ndjson")
        await gcs_client.upload_ndjson(archived, f"{base_path}/archived.ndjson")

    @staticmethod
    def _proposal_liveness(state: Optional[str]) -> str:
        if state in {"pending", "active"}:
            return "live"
        if state in {"deleted"}:
            return "deleted"
        return "archived"

    @staticmethod
    def _proposal_id_from_name(blob_name: str) -> str:
        filename = blob_name.split("/")[-1]
        return filename.replace(".json.gz", "").replace(".json", "")

    @staticmethod
    def _as_json_name(blob_name: str) -> str:
        if blob_name.endswith(".json"):
            return blob_name
        if blob_name.endswith(".json.gz"):
            return blob_name[:-3]
        return blob_name

    @staticmethod
    def _cache_control(liveness: str) -> str:
        if liveness == "live":
            if ENVIRONMENT == "production":
                max_age = 30 * SCHEDULER_INTERVAL_MINUTES
            else:
                max_age = 10 * SCHEDULER_INTERVAL_MINUTES
        elif liveness == "archived":
            if ENVIRONMENT == "production":
                max_age = 365 * 24 * 60 * 60
            else:
                max_age = 2 * 60
        else:
            max_age = SCHEDULER_INTERVAL_MINUTES * 60
        return "public, max-age=" + str(max_age)

    @staticmethod
    def _compute_payload_hash(
        payload: Dict[str, Any],
        liveness: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        sanitized = copy.deepcopy(payload)
        sanitized.pop("data_eng_properties", None)
        hash_payload: Dict[str, Any] = {"payload": sanitized, "liveness": liveness}
        if extra:
            hash_payload["meta"] = extra
        return json_hash(hash_payload)
