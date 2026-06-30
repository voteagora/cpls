import asyncio
import copy
import sys
from itertools import count
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CPLS_DIR = PROJECT_ROOT / "cpls"
if str(CPLS_DIR) not in sys.path:
    sys.path.insert(0, str(CPLS_DIR))

from syncs import SnapshotSync


class FakeBlob:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeGCSClient:
    def __init__(self) -> None:
        self.storage = {}

    async def upload_dict(self, data, blob_name, metadata=None, cache_control=None):
        self.storage[blob_name] = {
            "data": copy.deepcopy(data),
            "metadata": copy.deepcopy(metadata) if metadata else {},
            "cache_control": cache_control,
        }
        return True

    async def read_dict(self, blob_name):
        entry = self.storage.get(blob_name)
        if not entry:
            return None
        return copy.deepcopy(entry["data"])

    async def list_blobs(self, prefix):
        return [FakeBlob(name) for name in self.storage if name.startswith(prefix)]

    async def upload_ndjson(self, data, blob_name, cache_control=None, metadata=None):
        self.storage[blob_name] = {
            "data": copy.deepcopy(data),
            "metadata": copy.deepcopy(metadata) if metadata else {},
            "cache_control": cache_control,
        }
        return True


class FakeSnapshotAPIClient:
    def __init__(self, sequences):
        self.sequences = sequences
        self.index = 0
        self.fetch_proposal_overrides = {}
        self.last_lookup = {}

    def fetch_space_proposals(self, space):
        if self.index < len(self.sequences):
            result = self.sequences[self.index]
        else:
            result = self.sequences[-1]
        self.index += 1
        self.last_lookup = {proposal["id"]: copy.deepcopy(proposal) for proposal in result}
        return copy.deepcopy(result)

    def fetch_proposal(self, proposal_id):
        if proposal_id in self.fetch_proposal_overrides:
            return copy.deepcopy(self.fetch_proposal_overrides[proposal_id])
        proposal = self.last_lookup.get(proposal_id)
        return copy.deepcopy(proposal)


def test_snapshot_sync_uploads_and_splits_by_liveness():
    proposals = [
        {
            "id": "1",
            "state": "active",
            "title": "Proposal Active",
            "body": "Long body",
            "choices": ["Yes", "No"],
            "start": 1,
            "end": 10,
            "scores": [1, 0],
            "scores_total": 1,
        },
        {
            "id": "2",
            "state": "closed",
            "title": "Proposal Closed",
            "body": "Another body",
            "choices": ["A", "B"],
            "start": 2,
            "end": 20,
            "scores": [0, 1],
            "scores_total": 1,
        },
    ]

    fake_api = FakeSnapshotAPIClient([proposals])
    fake_gcs = FakeGCSClient()
    now_counter = count(start=1000)

    sync = SnapshotSync("testspace", api_client=fake_api, now_provider=lambda: next(now_counter))

    asyncio.run(sync.refresh_list(fake_gcs))

    active_blob = fake_gcs.storage["data/testspace/proposal/snapshot/raw/1.json"]
    archived_blob = fake_gcs.storage["data/testspace/proposal/snapshot/raw/2.json"]

    assert active_blob["metadata"]["liveness"] == "live"
    assert archived_blob["metadata"]["liveness"] == "archived"

    live_list = fake_gcs.storage["data/testspace/proposal_list/snapshot/live.ndjson"]["data"]
    archived_list = fake_gcs.storage["data/testspace/proposal_list/snapshot/archived.ndjson"]["data"]

    assert len(live_list) == 1
    assert len(archived_list) == 1
    assert "body" not in live_list[0]
    assert "hash" not in live_list[0]["data_eng_properties"]


def test_snapshot_sync_marks_deleted_after_consecutive_misses():
    initial_proposals = [
        {
            "id": "p1",
            "state": "active",
            "title": "Keep me",
            "body": "Body",
            "choices": ["Y", "N"],
            "start": 3,
            "end": 30,
        }
    ]

    fake_api = FakeSnapshotAPIClient([initial_proposals, []])
    fake_gcs = FakeGCSClient()
    now_counter = count(start=2000)

    sync = SnapshotSync("testspace", api_client=fake_api, now_provider=lambda: next(now_counter))

    asyncio.run(sync.refresh_list(fake_gcs))

    # Gone from the list AND null on a direct lookup -> a confirmed miss.
    fake_api.fetch_proposal_overrides["p1"] = None
    blob_name = "data/testspace/proposal/snapshot/raw/p1.json"

    # First miss: deferred (strike recorded), not yet deleted.
    asyncio.run(sync.refresh_list(fake_gcs))
    props = fake_gcs.storage[blob_name]["data"]["data_eng_properties"]
    assert props["liveness"] == "live"
    assert props["missing_count"] == 1

    # Second consecutive miss: retired.
    asyncio.run(sync.refresh_list(fake_gcs))
    props = fake_gcs.storage[blob_name]["data"]["data_eng_properties"]
    assert props["liveness"] == "deleted"
    assert "deleted_at" in props

    live_list = fake_gcs.storage["data/testspace/proposal_list/snapshot/live.ndjson"]["data"]
    assert live_list == []


def test_snapshot_sync_keeps_flagged_proposal_alive():
    proposal = {
        "id": "p1",
        "state": "closed",
        "title": "Flagged but alive",
        "body": "Body",
        "choices": ["Y", "N"],
        "start": 3,
        "end": 30,
    }

    fake_api = FakeSnapshotAPIClient([[proposal], []])
    fake_gcs = FakeGCSClient()
    now_counter = count(start=5000)

    sync = SnapshotSync("testspace", api_client=fake_api, now_provider=lambda: next(now_counter))

    asyncio.run(sync.refresh_list(fake_gcs))

    # Flagged -> excluded from the list, but still resolves by id.
    fake_api.fetch_proposal_overrides["p1"] = dict(proposal)
    blob_name = "data/testspace/proposal/snapshot/raw/p1.json"

    # Repeated absence from the list must not delete a proposal that still exists.
    asyncio.run(sync.refresh_list(fake_gcs))
    asyncio.run(sync.refresh_list(fake_gcs))

    props = fake_gcs.storage[blob_name]["data"]["data_eng_properties"]
    assert props["liveness"] != "deleted"
    assert props.get("missing_count", 0) == 0


def test_snapshot_sync_resets_strikes_when_proposal_returns():
    proposal = {
        "id": "p1",
        "state": "active",
        "title": "Flaky",
        "body": "Body",
        "choices": ["Y", "N"],
        "start": 3,
        "end": 30,
    }

    fake_api = FakeSnapshotAPIClient([[proposal], [], [proposal]])
    fake_gcs = FakeGCSClient()
    now_counter = count(start=6000)

    sync = SnapshotSync("testspace", api_client=fake_api, now_provider=lambda: next(now_counter))

    asyncio.run(sync.refresh_list(fake_gcs))

    # Transiently absent from both list and id lookup -> one strike.
    fake_api.fetch_proposal_overrides["p1"] = None
    blob_name = "data/testspace/proposal/snapshot/raw/p1.json"
    asyncio.run(sync.refresh_list(fake_gcs))
    props = fake_gcs.storage[blob_name]["data"]["data_eng_properties"]
    assert props["liveness"] == "live"
    assert props["missing_count"] == 1

    # Reappears in the list -> strike cleared, never deleted.
    del fake_api.fetch_proposal_overrides["p1"]
    asyncio.run(sync.refresh_list(fake_gcs))
    props = fake_gcs.storage[blob_name]["data"]["data_eng_properties"]
    assert props["liveness"] == "live"
    assert props.get("missing_count", 0) == 0


def test_snapshot_sync_updates_when_votes_change():
    proposal = {
        "id": "p2",
        "state": "active",
        "title": "Voting update",
        "body": "Body",
        "choices": ["Yes", "No"],
        "start": 4,
        "end": 40,
        "scores": [10, 2],
        "scores_total": 12,
        "votes": 12,
    }

    fake_api = FakeSnapshotAPIClient([[proposal], [dict(proposal, votes=13, scores_total=13, scores=[11, 2])]])
    fake_gcs = FakeGCSClient()
    now_counter = count(start=3000)

    sync = SnapshotSync("testspace", api_client=fake_api, now_provider=lambda: next(now_counter))

    asyncio.run(sync.refresh_list(fake_gcs))
    first_hash = fake_gcs.storage["data/testspace/proposal/snapshot/raw/p2.json"]["metadata"]["hash"]

    asyncio.run(sync.refresh_list(fake_gcs))
    updated_blob = fake_gcs.storage["data/testspace/proposal/snapshot/raw/p2.json"]
    second_hash = updated_blob["metadata"]["hash"]

    assert first_hash != second_hash
    assert updated_blob["data"]["votes"] == 13
    assert updated_blob["data"]["scores_total"] == 13
