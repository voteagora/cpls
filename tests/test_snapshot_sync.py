"""Tests for SnapshotSync deletion detection (sync_snapshot._reconcile_deleted_proposals)."""

import copy

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

SAMPLE_CONFIG = {
    "schema": "ens",
    "dao_slug": "ENS",
    "index_tenant_prefix": "en",
    "features": {"snapshot_proposals": True},
    "deployment": {
        "chain_id": 1,
        "gov": {"address": "0xGov"},
        "token": {"address": "0xToken"},
    },
}

PREFIX = "data/ens/proposal/snapshot/raw/"


class FakeBlob:
    def __init__(self, name):
        self.name = name


class FakeGCS:
    def __init__(self):
        self.storage = {}

    async def list_blobs(self, prefix):
        return [FakeBlob(name) for name in list(self.storage) if name.startswith(prefix)]

    async def read_dict(self, name):
        entry = self.storage.get(name)
        return copy.deepcopy(entry) if entry is not None else None

    async def upload_dict(self, data, blob_name, metadata=None, cache_control=None):
        self.storage[blob_name] = copy.deepcopy(data)
        return True


def make_sync():
    with patch("cpls.sync.PostgreSQLClient"), patch("cpls.sync.BlockCacheClient"), patch(
        "cpls.sync.load_tenant_config", return_value=SAMPLE_CONFIG
    ):
        from cpls.sync_snapshot import SnapshotSync

        sync = SnapshotSync("ens", SAMPLE_CONFIG, reset=False, http_client=AsyncMock())
    sync.sc = MagicMock()
    sync.sc.get_proposal = AsyncMock(return_value=None)
    return sync


def seed(gcs, proposal_id="p1", liveness="live"):
    name = f"{PREFIX}{proposal_id}.json.gz"
    gcs.storage[name] = {
        "id": proposal_id,
        "title": "T",
        "num_of_votes": 3,
        "data_eng_properties": {"liveness": liveness, "source": "snapshot", "hash": "h0"},
    }
    return name


def props(gcs, name):
    return gcs.storage[name]["data_eng_properties"]


@pytest.mark.asyncio
async def test_retires_after_consecutive_misses():
    sync = make_sync()
    gcs = FakeGCS()
    name = seed(gcs)
    sync.sc.get_proposal = AsyncMock(return_value=None)  # confirmed gone from Snapshot

    # First miss: deferred (strike), not yet deleted.
    assert await sync._reconcile_deleted_proposals(set(), gcs) is True
    assert props(gcs, name)["liveness"] == "live"
    assert props(gcs, name)["missing_count"] == 1

    # Second consecutive miss: retired.
    assert await sync._reconcile_deleted_proposals(set(), gcs) is True
    assert props(gcs, name)["liveness"] == "deleted"
    assert "deleted_at" in props(gcs, name)


@pytest.mark.asyncio
async def test_flagged_proposal_stays_alive():
    sync = make_sync()
    gcs = FakeGCS()
    name = seed(gcs)
    # Flagged -> dropped from the flagged:false list, but still resolves by id.
    sync.sc.get_proposal = AsyncMock(return_value={"id": "p1", "state": "closed"})

    await sync._reconcile_deleted_proposals(set(), gcs)
    await sync._reconcile_deleted_proposals(set(), gcs)

    assert props(gcs, name)["liveness"] == "live"
    assert props(gcs, name).get("missing_count", 0) == 0


@pytest.mark.asyncio
async def test_strike_resets_when_seen_again():
    sync = make_sync()
    gcs = FakeGCS()
    name = seed(gcs)
    sync.sc.get_proposal = AsyncMock(return_value=None)

    await sync._reconcile_deleted_proposals(set(), gcs)  # strike 1
    assert props(gcs, name)["missing_count"] == 1

    await sync._reconcile_deleted_proposals({"p1"}, gcs)  # back in the list -> reset
    assert props(gcs, name)["liveness"] == "live"
    assert props(gcs, name).get("missing_count", 0) == 0


@pytest.mark.asyncio
async def test_archived_proposals_are_not_reconciled():
    sync = make_sync()
    gcs = FakeGCS()
    name = seed(gcs, liveness="archived")
    sync.sc.get_proposal = AsyncMock(return_value=None)

    assert await sync._reconcile_deleted_proposals(set(), gcs) is False
    assert props(gcs, name)["liveness"] == "archived"
    sync.sc.get_proposal.assert_not_called()


def _client_with_response(json_body, status_error=None):
    from cpls.sync_snapshot import SnapshotGraphQLClient

    resp = MagicMock()
    resp.raise_for_status = MagicMock(side_effect=status_error)
    resp.json = MagicMock(return_value=json_body)
    http = AsyncMock()
    http.post = AsyncMock(return_value=resp)
    return SnapshotGraphQLClient("ens", http)


@pytest.mark.asyncio
async def test_get_proposal_returns_none_only_for_genuine_deletion():
    client = _client_with_response({"data": {"item": None}})
    assert await client.get_proposal("p1") is None


@pytest.mark.asyncio
async def test_get_proposal_returns_proposal_when_present():
    client = _client_with_response({"data": {"item": {"id": "p1", "state": "closed"}}})
    assert await client.get_proposal("p1") == {"id": "p1", "state": "closed"}


@pytest.mark.asyncio
async def test_get_proposal_raises_on_graphql_error_payload():
    # HTTP 200 with errors and no data must NOT be read as a deletion.
    client = _client_with_response({"errors": [{"message": "boom"}]})
    with pytest.raises(Exception):
        await client.get_proposal("p1")


@pytest.mark.asyncio
async def test_get_proposal_raises_on_http_error():
    client = _client_with_response({}, status_error=RuntimeError("500"))
    with pytest.raises(Exception):
        await client.get_proposal("p1")


@pytest.mark.asyncio
async def test_api_error_never_deletes():
    sync = make_sync()
    gcs = FakeGCS()
    name = seed(gcs)
    sync.sc.get_proposal = AsyncMock(side_effect=Exception("boom"))

    assert await sync._reconcile_deleted_proposals(set(), gcs) is False
    assert props(gcs, name)["liveness"] == "live"
    assert props(gcs, name).get("missing_count", 0) == 0
