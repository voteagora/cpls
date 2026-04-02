"""
Targeted tests for SnapshotSync.refresh_list branches:
- govless_proposal_blob_name (line 145)
- archived skip when reset=False (line 171)
- hash unchanged skip (lines 183-186)
- reuse_tally path (line 201)
- state not 'active' or 'closed' raises (line 250)
"""

import pytest
import time
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


class FakeAcquire:
    def __init__(self, conn):
        self._conn = conn
    async def __aenter__(self):
        return self._conn
    async def __aexit__(self, *args):
        return False


@pytest.fixture
def snapshot_sync():
    with patch("cpls.sync.PostgreSQLClient") as MockPG, \
         patch("cpls.sync.BlockCacheClient") as MockBC, \
         patch("cpls.sync.load_tenant_config", return_value=SAMPLE_CONFIG):

        mock_http = AsyncMock()
        mock_bc = MagicMock()
        mock_bc.clear_lru = MagicMock()
        MockBC.return_value = mock_bc

        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = MagicMock()
        mock_pool.acquire.return_value = FakeAcquire(mock_conn)
        mock_pg = MagicMock()
        mock_pg.connect = AsyncMock(return_value=mock_pool)
        MockPG.return_value = mock_pg

        from cpls.sync_snapshot import SnapshotSync
        sync = SnapshotSync("ens", SAMPLE_CONFIG, reset=False, http_client=mock_http)
        sync.bc = mock_bc
        sync.pg = mock_pg

        yield sync, mock_bc


@pytest.fixture
def mock_gcs():
    gcs = MagicMock()
    gcs.upload_dict = AsyncMock(return_value=True)
    gcs.upload_ndjson = AsyncMock(return_value=True)
    gcs.read_ndjson = AsyncMock(return_value=None)
    gcs.read_dict = AsyncMock(return_value=None)
    gcs.list_blobs = AsyncMock(return_value=[])
    return gcs


def make_proposal(proposal_id="p1", state="active", num_votes=0):
    now = int(time.time())
    return {
        "id": proposal_id,
        "author": "0xAuthor",
        "body": "# Snapshot\n\nBody text",
        "choices": ["For", "Against"],
        "created": now - 7200,
        "end": now + 3600 if state == "active" else now - 3600,
        "link": "https://snapshot.org",
        "network": "1",
        "scores": [10, 5],
        "scores_state": "final",
        "scores_total": 15,
        "scores_updated": now - 100,
        "snapshot": "18000000",
        "start": now - 3600,
        "state": state,
        "title": "Test Proposal",
        "type": "basic",
        "votes": num_votes,
    }


class TestGovlessBlobName:

    def test_govless_proposal_blob_name(self, snapshot_sync):
        """Line 145: govless_proposal_blob_name returns correct path."""
        sync, _ = snapshot_sync
        result = sync.govless_proposal_blob_name("my-proposal-id")
        assert result == "data/ens/proposal/snapshot/raw/my-proposal-id.json.gz"


class TestArchivedSkip:

    @pytest.mark.asyncio
    async def test_archived_proposal_skipped_when_reset_false(self, snapshot_sync, mock_gcs):
        """Line 171: archived proposals are skipped when reset=False."""
        sync, _ = snapshot_sync
        sync.reset = False

        proposals = [make_proposal("p-arch", state="closed")]
        sync.sc = MagicMock()
        sync.sc.get_proposals = AsyncMock(return_value=proposals)

        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.metadata = {"liveness": "archived", "hash": "oldhash", "num_of_votes": "0"}
        mock_gcs.get_blob = AsyncMock(return_value=mock_blob)

        result = await sync.refresh_list(mock_gcs)
        assert result["refreshed"] == 0
        mock_gcs.upload_dict.assert_not_called()

    @pytest.mark.asyncio
    async def test_archived_not_skipped_when_reset_true(self, snapshot_sync, mock_gcs):
        """archived proposals are processed when reset=True."""
        sync, _ = snapshot_sync
        sync.reset = True

        proposals = [make_proposal("p-arch-reset", state="closed")]
        sync.sc = MagicMock()
        sync.sc.get_proposals = AsyncMock(return_value=proposals)

        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.metadata = {"liveness": "archived", "hash": "oldhash", "num_of_votes": "0"}
        mock_gcs.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs.list_blobs = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs)
        assert result["refreshed"] == 1


class TestHashUnchangedSkip:

    @pytest.mark.asyncio
    async def test_hash_unchanged_skips_proposal(self, snapshot_sync, mock_gcs):
        """Lines 183-186: hash unchanged → SkipProposal → skipped_count++."""
        sync, _ = snapshot_sync
        sync.reset = False

        proposals = [make_proposal("p-hash", state="active", num_votes=5)]
        sync.sc = MagicMock()
        sync.sc.get_proposals = AsyncMock(return_value=proposals)

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs.list_blobs = AsyncMock(return_value=[])

        from cpls.sync import SkipProposal
        with patch.object(sync, 'check_existing_proposal_hash',
                          side_effect=SkipProposal("unchanged", proposal_id="p-hash")):
            result = await sync.refresh_list(mock_gcs)

        assert result["skipped"] == 1
        assert result["refreshed"] == 0


class TestReuseTally:

    @pytest.mark.asyncio
    async def test_reuse_tally_skips_vote_fetch(self, snapshot_sync, mock_gcs):
        """Line 201: when num_of_votes matches, reuse_tally=True → no vote fetch needed."""
        sync, _ = snapshot_sync
        sync.reset = False

        proposals = [make_proposal("p-reuse", state="active", num_votes=3)]
        sync.sc = MagicMock()
        sync.sc.get_proposals = AsyncMock(return_value=proposals)
        sync.sc.get_all_votes = AsyncMock()

        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.metadata = {"liveness": "live", "hash": "different-hash", "num_of_votes": "3"}
        mock_gcs.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs.list_blobs = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs)
        assert result["refreshed"] == 1
        sync.sc.get_all_votes.assert_not_called()


class TestStateNotActiveOrClosed:

    @pytest.mark.asyncio
    async def test_unknown_state_raises(self, snapshot_sync, mock_gcs):
        """Line 250: state not 'active' or 'closed' raises Exception."""
        sync, _ = snapshot_sync

        proposal = make_proposal("p-pending", state="pending", num_votes=0)
        proposal["state"] = "pending"
        sync.sc = MagicMock()
        sync.sc.get_proposals = AsyncMock(return_value=[proposal])

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs.list_blobs = AsyncMock(return_value=[])

        with pytest.raises(Exception, match="Proposal state is not closed"):
            await sync.refresh_list(mock_gcs)
