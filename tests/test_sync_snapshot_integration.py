"""
Integration tests for SnapshotSync.refresh_list() with fully mocked externals.
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
        "gov": {"address": "0xGovAddr"},
        "token": {"address": "0xTokenAddr"},
    },
}


def make_snapshot_proposal(proposal_id="snap-1", state="active", ptype="basic",
                           votes=0, start=None, end=None):
    now = int(time.time())
    return {
        "id": proposal_id,
        "author": "0xAuthor",
        "body": "# Test Proposal\n\nBody",
        "choices": ["For", "Against", "Abstain"],
        "created": now - 7200,
        "end": end or now + 3600,
        "start": start or now - 3600,
        "link": f"https://snapshot.org/#/ens.eth/proposal/{proposal_id}",
        "network": "1",
        "scores": [100, 50, 10],
        "scores_state": "final",
        "scores_total": 160,
        "scores_updated": now - 100,
        "snapshot": "18000000",
        "state": state,
        "title": "Test Proposal",
        "type": ptype,
        "votes": votes,
    }


class FakeAcquire:
    def __init__(self, conn):
        self._conn = conn
    async def __aenter__(self):
        return self._conn
    async def __aexit__(self, *args):
        return False


@pytest.fixture
def mocked_snapshot():
    with patch("cpls.sync.PostgreSQLClient") as MockPG, \
         patch("cpls.sync.BlockCacheClient") as MockBC, \
         patch("cpls.sync.load_tenant_config", return_value=SAMPLE_CONFIG), \
         patch("cpls.sync_snapshot.SnapshotGraphQLClient") as MockSGQL:

        mock_http = AsyncMock()
        mock_bc = MagicMock()
        mock_bc.clear_lru = MagicMock()
        mock_bc.get_blocktime = AsyncMock(return_value=int(time.time()) - 3600)
        mock_bc.get_ens_lru = AsyncMock(return_value=None)
        MockBC.return_value = mock_bc

        mock_pg = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = MagicMock()
        mock_pool.acquire.return_value = FakeAcquire(mock_conn)
        mock_pg.connect = AsyncMock(return_value=mock_pool)
        MockPG.return_value = mock_pg

        mock_sgql = MagicMock()
        mock_sgql.get_proposals = AsyncMock(return_value=[])
        mock_sgql.get_all_votes = AsyncMock(return_value=[])
        MockSGQL.return_value = mock_sgql

        from cpls.sync_snapshot import SnapshotSync
        sync = SnapshotSync("ens", SAMPLE_CONFIG, reset=False, http_client=mock_http)
        sync.bc = mock_bc
        sync.pg = mock_pg
        sync.sc = mock_sgql

        yield sync, mock_sgql, mock_bc, mock_pg, mock_conn


class TestSnapshotRefreshListNewProposal:

    @pytest.mark.asyncio
    async def test_active_proposal_processed(self, mocked_snapshot, mock_gcs_client):
        sync, mock_sgql, mock_bc, mock_pg, mock_conn = mocked_snapshot

        proposal = make_snapshot_proposal("snap-1", state="active", votes=3)
        mock_sgql.get_proposals = AsyncMock(return_value=[proposal])

        # GCS: proposal doesn't exist yet
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 1
        assert result["skipped"] == 0
        mock_gcs_client.upload_dict.assert_called()

        # Verify the uploaded proposal has correct enrichment
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["start_blocktime"] == uploaded["start"]
        assert uploaded["end_blocktime"] == uploaded["end"]
        assert "description" in uploaded
        assert "body" not in uploaded  # body renamed to description


class TestSnapshotRefreshListClosed:

    @pytest.mark.asyncio
    async def test_closed_proposal_archived(self, mocked_snapshot, mock_gcs_client):
        sync, mock_sgql, mock_bc, mock_pg, mock_conn = mocked_snapshot

        now = int(time.time())
        proposal = make_snapshot_proposal("snap-2", state="closed", votes=10,
                                          start=now - 7200, end=now - 3600)
        mock_sgql.get_proposals = AsyncMock(return_value=[proposal])

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 1
        # Check cache control is for archived
        upload_call = mock_gcs_client.upload_dict.call_args_list[0]
        metadata = upload_call[1]["metadata"]
        assert metadata["liveness"] == "archived"


class TestSnapshotRefreshListSkipArchived:

    @pytest.mark.asyncio
    async def test_existing_archived_proposal_skipped(self, mocked_snapshot, mock_gcs_client):
        sync, mock_sgql, mock_bc, mock_pg, mock_conn = mocked_snapshot

        proposal = make_snapshot_proposal("snap-3", state="closed", votes=5)
        mock_sgql.get_proposals = AsyncMock(return_value=[proposal])

        # GCS: proposal exists and is archived
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.metadata = {"liveness": "archived", "hash": "abc", "num_of_votes": "5"}
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)

        result = await sync.refresh_list(mock_gcs_client)

        assert result["skipped"] == 1
        assert result["refreshed"] == 0
        mock_gcs_client.upload_dict.assert_not_called()


class TestSnapshotRefreshListCopeland:

    @pytest.mark.asyncio
    async def test_copeland_votes_processed(self, mocked_snapshot, mock_gcs_client):
        sync, mock_sgql, mock_bc, mock_pg, mock_conn = mocked_snapshot

        proposal = make_snapshot_proposal("snap-4", state="active", ptype="copeland", votes=2)
        mock_sgql.get_proposals = AsyncMock(return_value=[proposal])

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])
        mock_gcs_client.read_ndjson = AsyncMock(return_value=None)

        # DB calls: get_vp_snapshot_all_delegates_from_db returns VP data,
        # get_delegate_metadata returns delegate statement rows
        vp_call_count = [0]
        async def multi_fetch(qry=None, *args, **kwargs):
            vp_call_count[0] += 1
            if "delegate_statements" in str(qry):
                return []  # no delegate metadata
            return [
                {"addr": "0xvoter1", "vp": 1000},
                {"addr": "0xvoter2", "vp": 2000},
                {"addr": "0xnonvoter", "vp": 500},
            ]
        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

        # Votes from Snapshot API
        mock_sgql.get_all_votes = AsyncMock(return_value=[
            {"voter": "0xVoter1", "vp": 1000, "choice": [[1, 2]], "created": 100, "reason": "", "app": "snapshot", "id": "v1", "vp_by_strategy": [1000], "vp_state": "final"},
            {"voter": "0xVoter2", "vp": 2000, "choice": [[2, 1]], "created": 101, "reason": "", "app": "snapshot", "id": "v2", "vp_by_strategy": [2000], "vp_state": "final"},
        ])

        # Nonivotes returns empty
        mock_response = MagicMock()
        mock_response.status_code = 404
        sync.http_client.get = AsyncMock(return_value=mock_response)

        result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 1
        # Should have uploaded votes and hasnt_voted
        ndjson_calls = mock_gcs_client.upload_ndjson.call_args_list
        blob_names = [c[0][1] for c in ndjson_calls]
        assert any("votes" in b for b in blob_names)
        assert any("hasnt_voted" in b for b in blob_names)
