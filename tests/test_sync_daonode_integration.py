"""
Integration tests for DaoNodeSync.refresh_list() with fully mocked externals.
Tests the complete proposal processing pipeline: fetch → hash check → enrich → upload.
"""

import pytest
import time
from unittest.mock import AsyncMock, MagicMock, patch


SAMPLE_CONFIG = {
    "schema": "optimism",
    "dao_slug": "Optimism",
    "index_tenant_prefix": "op",
    "features": {"dao_node_proposals": True},
    "deployment": {
        "chain_id": 10,
        "gov": {"address": "0xGovAddr"},
        "token": {"address": "0xTokenAddr"},
    },
}


def make_proposal_info(proposal_id="99999"):
    return {"id": proposal_id}


def make_proposal_detail(proposal_id="99999", start_block=100, end_block=200):
    return {
        "proposal": {
            "id": proposal_id,
            "description": "# Test Proposal\n\nBody text",
            "start_block": start_block,
            "end_block": end_block,
            "block_number": 90,
            "transaction_index": 0,
            "voting_module_name": "standard",
            "proposer": "0xProposer",
        }
    }


class FakeAcquire:
    """Async context manager for pool.acquire()"""
    def __init__(self, conn):
        self._conn = conn
    async def __aenter__(self):
        return self._conn
    async def __aexit__(self, *args):
        return False


@pytest.fixture
def mocked_daonode():
    """Create a DaoNodeSync with all externals mocked."""
    with patch("cpls.sync.PostgreSQLClient") as MockPG, \
         patch("cpls.sync.BlockCacheClient") as MockBC, \
         patch("cpls.sync.load_tenant_config", return_value=SAMPLE_CONFIG):

        mock_http = AsyncMock()
        mock_bc = MagicMock()
        mock_bc.clear_lru = MagicMock()
        mock_bc.get_blocktime = AsyncMock(return_value=int(time.time()) - 3600)
        mock_bc.get_exact_blocktime = AsyncMock(return_value=int(time.time()) - 3600)
        mock_bc.get_estimated_blocktime = AsyncMock(return_value=int(time.time()) - 3600)
        mock_bc.get_ens_lru = AsyncMock(return_value=None)
        mock_bc.contract_call_encoded = AsyncMock(return_value={
            "result": "0x0000000000000000000000000000000000000000000000000000000000000004"  # SUCCEEDED state
        })
        mock_bc.votable_supply_at_block_with_oracle = AsyncMock(return_value=1000000)
        mock_bc.get_transaction_by_index = AsyncMock(return_value={"tx": "0xtxhash"})
        MockBC.return_value = mock_bc

        mock_pg = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.fetchrow = AsyncMock(return_value={"votable_supply": "1000000"})
        mock_pool = MagicMock()
        mock_pool.acquire.return_value = FakeAcquire(mock_conn)
        mock_pg.connect = AsyncMock(return_value=mock_pool)
        MockPG.return_value = mock_pg

        from cpls.sync_daonode import DaoNodeSync
        sync = DaoNodeSync("optimism", SAMPLE_CONFIG, reset=False, http_client=mock_http)
        sync.bc = mock_bc
        sync.pg = mock_pg

        yield sync, mock_http, mock_bc, mock_pg, mock_conn


class TestDaoNodeRefreshListNewProposal:
    """Test refresh_list with a brand new proposal that doesn't exist in GCS yet."""

    @pytest.mark.asyncio
    async def test_new_proposal_gets_processed(self, mocked_daonode, mock_gcs_client):
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_daonode

        # Setup: DaoNode API returns one proposal
        progress_resp = MagicMock()
        progress_resp.json.return_value = {"block": 500}
        proposals_resp = MagicMock()
        proposals_resp.json.return_value = {"proposals": [make_proposal_info("99999")]}
        detail_resp = MagicMock()
        detail_resp.json.return_value = make_proposal_detail("99999", start_block=100, end_block=200)

        mock_http.get = AsyncMock(side_effect=[progress_resp, proposals_resp, detail_resp])

        # Setup: GCS says proposal doesn't exist yet
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])

        # Setup: No votes in DB
        mock_conn.fetch = AsyncMock(return_value=[])

        # Setup: BlockCache returns timestamps in the past (proposal ended)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 1
        assert result["skipped"] == 0
        # Proposal should have been uploaded
        mock_gcs_client.upload_dict.assert_called()
        # Source list and full list should have been refreshed
        assert mock_gcs_client.upload_ndjson.call_count >= 1


class TestDaoNodeRefreshListSkipUnchanged:
    """Test that unchanged proposals are skipped via hash comparison."""

    @pytest.mark.asyncio
    async def test_unchanged_proposal_is_skipped(self, mocked_daonode, mock_gcs_client):
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_daonode

        # Setup: DaoNode API
        progress_resp = MagicMock()
        progress_resp.json.return_value = {"block": 500}
        proposals_resp = MagicMock()
        proposals_resp.json.return_value = {"proposals": [make_proposal_info("99999")]}
        detail_resp = MagicMock()
        detail = make_proposal_detail("99999", start_block=100, end_block=200)
        detail_resp.json.return_value = detail

        mock_http.get = AsyncMock(side_effect=[progress_resp, proposals_resp, detail_resp])

        # Setup: GCS says proposal exists and is archived
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.metadata = {"liveness": "archived", "hash": "somehash", "num_of_votes": "0"}
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)

        result = await sync.refresh_list(mock_gcs_client)

        # Should have been skipped (archived)
        assert result["skipped"] == 1
        assert result["refreshed"] == 0
        mock_gcs_client.upload_dict.assert_not_called()


class TestDaoNodeRefreshListCensoredProposal:
    """Test that censored proposals are skipped."""

    @pytest.mark.asyncio
    async def test_censored_proposal_skipped(self, mocked_daonode, mock_gcs_client):
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_daonode

        censored_id = "1527730988371215275825638462098143152903080362182102606740072057386997269957"

        progress_resp = MagicMock()
        progress_resp.json.return_value = {"block": 500}
        proposals_resp = MagicMock()
        proposals_resp.json.return_value = {"proposals": [make_proposal_info(censored_id)]}

        mock_http.get = AsyncMock(side_effect=[progress_resp, proposals_resp])

        result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 0
        assert result["skipped"] == 0  # Censored proposals are `continue`d, not counted as skipped


class TestDaoNodeReadQuorum:
    """Test read_quorum for various DAO-specific branching."""

    @pytest.fixture
    def sync_for_quorum(self, mocked_daonode):
        sync, _, _, _, _ = mocked_daonode
        return sync

    @pytest.mark.asyncio
    async def test_uniswap_returns_fixed_quorum(self, sync_for_quorum):
        sync_for_quorum.infra_dao_slug = "uniswap"
        result = await sync_for_quorum.read_quorum({"id": "1", "start_block": 100})
        assert result == "40000000000000000000000000"

    @pytest.mark.asyncio
    async def test_optimism_pre_v6_returns_zero(self, sync_for_quorum):
        sync_for_quorum.infra_dao_slug = "optimism"
        sync_for_quorum.chain_id = 10
        result = await sync_for_quorum.read_quorum({"id": "1", "start_block": 100})
        assert result == "0"

    @pytest.mark.asyncio
    async def test_optimism_post_v6_uses_contract(self, sync_for_quorum):
        sync_for_quorum.infra_dao_slug = "optimism"
        sync_for_quorum.chain_id = 10
        sync_for_quorum.bc.contract_call_encoded = AsyncMock(
            return_value={"result": "0x00000000000000000000000000000000000000000000000000000000000003e8"}
        )
        result = await sync_for_quorum.read_quorum({"id": "1", "start_block": 200000000})
        assert result == "1000"  # 0x3e8 = 1000

    @pytest.mark.asyncio
    async def test_default_dao_uses_contract(self, sync_for_quorum):
        sync_for_quorum.infra_dao_slug = "syndicate"
        sync_for_quorum.bc.contract_call_encoded = AsyncMock(
            return_value={"result": "0x00000000000000000000000000000000000000000000000000000000000001f4"}
        )
        result = await sync_for_quorum.read_quorum({"id": "42", "start_block": 100})
        assert result == "500"  # 0x1f4 = 500


class TestDaoNodeLifecycleStages:
    """Test lifecycle stage assignment for various proposal states."""

    @pytest.mark.asyncio
    async def test_active_proposal(self, mocked_daonode, mock_gcs_client):
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_daonode

        now = int(time.time())
        start_block, end_block = 100, 200

        progress_resp = MagicMock()
        progress_resp.json.return_value = {"block": 500}
        proposals_resp = MagicMock()
        proposals_resp.json.return_value = {"proposals": [make_proposal_info("11111")]}
        detail_resp = MagicMock()
        detail_resp.json.return_value = make_proposal_detail("11111", start_block, end_block)

        mock_http.get = AsyncMock(side_effect=[progress_resp, proposals_resp, detail_resp])

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])

        # start in the past, end in the future → ACTIVE
        mock_bc.get_blocktime = AsyncMock(side_effect=lambda chain_id, block: now - 3600 if block == start_block else now + 3600)

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1

        # Check the uploaded proposal has lifecycle_stage = ACTIVE
        upload_call = mock_gcs_client.upload_dict.call_args_list[0]
        uploaded_proposal = upload_call[0][0]
        assert uploaded_proposal["lifecycle_stage"] == "ACTIVE"
