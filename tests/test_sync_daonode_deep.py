"""
Deep coverage tests for DaoNodeSync: vote processing, lifecycle stages from
contract state, hybrid proposals, quorum branches, and votable supply.
"""

import pytest
import time
import copy
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


class FakeAcquire:
    def __init__(self, conn):
        self._conn = conn
    async def __aenter__(self):
        return self._conn
    async def __aexit__(self, *args):
        return False


@pytest.fixture
def daonode_sync():
    with patch("cpls.sync.PostgreSQLClient") as MockPG, \
         patch("cpls.sync.BlockCacheClient") as MockBC, \
         patch("cpls.sync.load_tenant_config", return_value=SAMPLE_CONFIG):

        mock_http = AsyncMock()
        mock_bc = MagicMock()
        mock_bc.clear_lru = MagicMock()
        mock_bc.get_blocktime = AsyncMock(return_value=int(time.time()) - 3600)
        mock_bc.get_estimated_blocktime = AsyncMock(return_value=int(time.time()) + 3600)
        mock_bc.get_ens_lru = AsyncMock(return_value=None)
        mock_bc.get_ens = AsyncMock(return_value=None)
        mock_bc.contract_call_encoded = AsyncMock(return_value={
            "result": "0x0000000000000000000000000000000000000000000000000000000000000004"
        })
        mock_bc.votable_supply_at_block_with_oracle = AsyncMock(return_value=1000000)
        mock_bc.votable_supply_at_block = AsyncMock(return_value=500000)
        mock_bc.get_transaction_by_index = AsyncMock(return_value={"tx": "0xtxhash"})
        mock_bc.last_block_before_timestamp = AsyncMock(return_value=18000050)
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


# ── read_quorum branches ──────────────────────────────────────────

class TestReadQuorumBranches:

    @pytest.mark.asyncio
    async def test_ens_quorum(self, daonode_sync):
        sync, _, mock_bc, _, _ = daonode_sync
        sync.infra_dao_slug = "ens"
        sync.chain_id = 1
        mock_bc.contract_call_encoded = AsyncMock(
            return_value={"result": "0x00000000000000000000000000000000000000000000000000000000000007d0"}
        )
        result = await sync.read_quorum({"id": "1", "start_block": 100})
        assert result == "2000"

    @pytest.mark.asyncio
    async def test_cyber_quorum(self, daonode_sync):
        sync, _, mock_bc, _, _ = daonode_sync
        sync.infra_dao_slug = "cyber"
        sync.chain_id = 7560
        mock_bc.contract_call_encoded = AsyncMock(
            return_value={"result": "0x00000000000000000000000000000000000000000000000000000000000493e0"}
        )
        result = await sync.read_quorum({"id": "1", "start_block": 100})
        assert result == "300000"
        mock_bc.contract_call_encoded.assert_awaited_once_with(
            7560, sync.gov_addr, 101, "quorum(uint256)", [1]
        )

    @pytest.mark.asyncio
    async def test_scroll_quorum(self, daonode_sync):
        sync, _, mock_bc, _, _ = daonode_sync
        sync.infra_dao_slug = "scroll"
        sync.chain_id = 534352
        # totalSupply returns 0x989680 = 10000000
        mock_bc.contract_call_encoded = AsyncMock(
            return_value={"result": "0x0000000000000000000000000000000000000000000000000000000000989680"}
        )
        result = await sync.read_quorum({
            "id": "1", "start_block": 100,
            "proposal_type_info": {"quorum": 2000}
        })
        # 10000000 * (2000 * 100000) / 1000000000 = 2000000
        assert result == "2000000"

    @pytest.mark.asyncio
    async def test_xai_quorum(self, daonode_sync):
        sync, _, mock_bc, _, _ = daonode_sync
        sync.infra_dao_slug = "xai"
        sync.chain_id = 660279
        mock_bc.contract_call_encoded = AsyncMock(
            return_value={"result": "0x0000000000000000000000000000000000000000000000000000000000000064"}
        )
        result = await sync.read_quorum({"id": "1", "start_block": 100})
        assert result == "100"

    @pytest.mark.asyncio
    async def test_pguild_quorum(self, daonode_sync):
        sync, _, mock_bc, _, _ = daonode_sync
        sync.infra_dao_slug = "pguild"
        sync.chain_id = 1
        mock_bc.contract_call_encoded = AsyncMock(
            return_value={"result": "0x00000000000000000000000000000000000000000000000000000000000000c8"}
        )
        result = await sync.read_quorum({"id": "1", "start_block": 100})
        assert result == "200"

    @pytest.mark.asyncio
    async def test_optimism_quorum_zero_fallback(self, daonode_sync):
        """When contract returns 0 quorum, fall back to 30% of votable supply."""
        sync, _, mock_bc, _, mock_conn = daonode_sync
        sync.infra_dao_slug = "optimism"
        sync.chain_id = 10
        mock_bc.contract_call_encoded = AsyncMock(
            return_value={"result": "0x0000000000000000000000000000000000000000000000000000000000000000"}
        )
        mock_bc.votable_supply_at_block_with_oracle = AsyncMock(return_value=1000000)
        result = await sync.read_quorum({"id": "1", "start_block": 200000000})
        assert result == "300000"  # 30% of 1000000


# ── read_snapshot_votable_supply branches ──────────────────────────

class TestVotableSupplyBranches:

    @pytest.mark.asyncio
    async def test_optimism_oracle_nonzero(self, daonode_sync):
        sync, _, mock_bc, _, _ = daonode_sync
        sync.infra_dao_slug = "optimism"
        mock_bc.votable_supply_at_block_with_oracle = AsyncMock(return_value=500000)
        result = await sync.read_snapshot_votable_supply(100)
        assert result == 500000

    @pytest.mark.asyncio
    async def test_optimism_oracle_zero_fallback_to_db(self, daonode_sync):
        sync, _, mock_bc, _, mock_conn = daonode_sync
        sync.infra_dao_slug = "optimism"
        mock_bc.votable_supply_at_block_with_oracle = AsyncMock(return_value=0)
        mock_conn.fetchrow = AsyncMock(return_value={"votable_supply": "750000"})
        result = await sync.read_snapshot_votable_supply(100)
        assert result == 750000

    @pytest.mark.asyncio
    async def test_scroll_uses_db(self, daonode_sync):
        sync, _, mock_bc, _, mock_conn = daonode_sync
        sync.infra_dao_slug = "scroll"
        mock_conn.fetchrow = AsyncMock(return_value={"votable_supply": "999"})
        result = await sync.read_snapshot_votable_supply(100)
        assert result == 999

    @pytest.mark.asyncio
    async def test_xai_uses_contract(self, daonode_sync):
        sync, _, mock_bc, _, _ = daonode_sync
        sync.infra_dao_slug = "xai"
        sync.chain_id = 660279
        mock_bc.get_blocktime = AsyncMock(return_value=int(time.time()) - 100)
        mock_bc.last_block_before_timestamp = AsyncMock(return_value=5000)
        mock_bc.contract_call_encoded = AsyncMock(
            return_value={"result": "0x00000000000000000000000000000000000000000000000000000000000186a0"}
        )
        result = await sync.read_snapshot_votable_supply(100)
        assert result == 100000

    @pytest.mark.asyncio
    async def test_default_uses_blockcache(self, daonode_sync):
        sync, _, mock_bc, _, _ = daonode_sync
        sync.infra_dao_slug = "syndicate"
        mock_bc.votable_supply_at_block = AsyncMock(return_value=123456)
        result = await sync.read_snapshot_votable_supply(100)
        assert result == 123456


# ── Vote processing with approval params ───────────────────────────

class TestVoteProcessing:

    @pytest.mark.asyncio
    async def test_approval_vote_params_decoded(self, daonode_sync, mock_gcs_client):
        """Approval votes have hex-encoded params that get decoded."""
        sync, mock_http, mock_bc, _, mock_conn = daonode_sync

        now = int(time.time())
        progress_resp = MagicMock()
        progress_resp.json.return_value = {"block": 500}
        proposals_resp = MagicMock()
        proposals_resp.json.return_value = {"proposals": [{"id": "77777"}]}
        detail_resp = MagicMock()
        detail_resp.json.return_value = {"proposal": {
            "id": "77777", "description": "# Approval Test",
            "start_block": 100, "end_block": 200,
            "block_number": 90, "transaction_index": 0,
            "voting_module_name": "approval",
            "proposer": "0xProposer",
        }}
        mock_http.get = AsyncMock(side_effect=[progress_resp, proposals_resp, detail_resp])

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])
        mock_gcs_client.read_ndjson = AsyncMock(return_value=None)

        past = now - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        # 3 uint256 values: length=1, then the actual param=2
        hex_params = "0000000000000000000000000000000000000000000000000000000000000020" \
                     "0000000000000000000000000000000000000000000000000000000000000001" \
                     "0000000000000000000000000000000000000000000000000000000000000002"

        fetch_count = [0]
        async def multi_fetch(qry=None, *args, **kwargs):
            fetch_count[0] += 1
            qry_str = str(qry)
            if "offchain_proposals" in qry_str:
                return []  # no hybrid mappings
            if "delegate_statements" in qry_str:
                return []
            if "token_delegate_votes_changed" in qry_str:
                return [{"addr": "0xvoter", "vp": 1000}]
            if "votes_" in qry_str or "proposal_id" in qry_str:
                return [{"transaction_hash": "0x1", "block_number": 150, "chain_id": 10,
                         "voter": "0xVoter", "support": 1, "weight": 1000,
                         "reason": "", "params": hex_params}]
            return []
        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1

        # Check votes ndjson was uploaded with decoded params
        ndjson_calls = [c for c in mock_gcs_client.upload_ndjson.call_args_list if "votes" in c[0][1]]
        assert len(ndjson_calls) >= 1
        votes_data = ndjson_calls[0][0][0]
        assert votes_data[0]["params"] == [2]


# ── Lifecycle from contract state ──────────────────────────────────

class TestDaoNodeLifecycleFromContract:

    @pytest.mark.asyncio
    async def test_defeated_proposal(self, daonode_sync, mock_gcs_client):
        """Ended proposal with state 3 → DEFEATED."""
        sync, mock_http, mock_bc, _, mock_conn = daonode_sync

        now = int(time.time())
        past = now - 7200

        progress_resp = MagicMock()
        progress_resp.json.return_value = {"block": 500}
        proposals_resp = MagicMock()
        proposals_resp.json.return_value = {"proposals": [{"id": "88888"}]}
        detail_resp = MagicMock()
        detail_resp.json.return_value = {"proposal": {
            "id": "88888", "description": "# Defeated Prop",
            "start_block": 100, "end_block": 200,
            "block_number": 90, "transaction_index": 0,
            "voting_module_name": "standard",
            "proposer": "0xProposer",
        }}
        mock_http.get = AsyncMock(side_effect=[progress_resp, proposals_resp, detail_resp])

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])
        mock_gcs_client.read_ndjson = AsyncMock(return_value=None)

        mock_bc.get_blocktime = AsyncMock(return_value=past)

        # Contract returns state 3 = DEFEATED
        mock_bc.contract_call_encoded = AsyncMock(
            return_value={"result": "0x0000000000000000000000000000000000000000000000000000000000000003"}
        )

        mock_conn.fetch = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "DEFEATED"

    @pytest.mark.asyncio
    async def test_queued_proposal(self, daonode_sync, mock_gcs_client):
        """Proposal with queue_event → QUEUED lifecycle."""
        sync, mock_http, mock_bc, _, mock_conn = daonode_sync

        now = int(time.time())
        past = now - 7200

        progress_resp = MagicMock()
        progress_resp.json.return_value = {"block": 500}
        proposals_resp = MagicMock()
        proposals_resp.json.return_value = {"proposals": [{"id": "66666"}]}
        detail_resp = MagicMock()
        detail_resp.json.return_value = {"proposal": {
            "id": "66666", "description": "# Queued Prop",
            "start_block": 100, "end_block": 200,
            "block_number": 90, "transaction_index": 0,
            "voting_module_name": "standard",
            "proposer": "0xProposer",
            "queue_event": {"block_number": 250, "transaction_index": 0},
        }}
        mock_http.get = AsyncMock(side_effect=[progress_resp, proposals_resp, detail_resp])

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])
        mock_gcs_client.read_ndjson = AsyncMock(return_value=None)

        mock_bc.get_blocktime = AsyncMock(return_value=past)
        mock_conn.fetch = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "QUEUED"

    @pytest.mark.asyncio
    async def test_cancelled_proposal(self, daonode_sync, mock_gcs_client):
        """Proposal with cancel_event → CANCELLED and archived."""
        sync, mock_http, mock_bc, _, mock_conn = daonode_sync

        now = int(time.time())
        past = now - 7200

        progress_resp = MagicMock()
        progress_resp.json.return_value = {"block": 500}
        proposals_resp = MagicMock()
        proposals_resp.json.return_value = {"proposals": [{"id": "55555"}]}
        detail_resp = MagicMock()
        detail_resp.json.return_value = {"proposal": {
            "id": "55555", "description": "# Cancelled Prop",
            "start_block": 100, "end_block": 200,
            "block_number": 90, "transaction_index": 0,
            "voting_module_name": "standard",
            "proposer": "0xProposer",
            "cancel_event": {"block_number": 150, "transaction_index": 0},
        }}
        mock_http.get = AsyncMock(side_effect=[progress_resp, proposals_resp, detail_resp])

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])
        mock_gcs_client.read_ndjson = AsyncMock(return_value=None)

        mock_bc.get_blocktime = AsyncMock(return_value=past)
        mock_conn.fetch = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "CANCELLED"
        metadata = mock_gcs_client.upload_dict.call_args_list[0][1]["metadata"]
        assert metadata["liveness"] == "archived"

    @pytest.mark.asyncio
    async def test_executed_proposal(self, daonode_sync, mock_gcs_client):
        """Proposal with execute_event → EXECUTED and archived."""
        sync, mock_http, mock_bc, _, mock_conn = daonode_sync

        now = int(time.time())
        past = now - 7200

        progress_resp = MagicMock()
        progress_resp.json.return_value = {"block": 500}
        proposals_resp = MagicMock()
        proposals_resp.json.return_value = {"proposals": [{"id": "44444"}]}
        detail_resp = MagicMock()
        detail_resp.json.return_value = {"proposal": {
            "id": "44444", "description": "# Executed Prop",
            "start_block": 100, "end_block": 200,
            "block_number": 90, "transaction_index": 0,
            "voting_module_name": "standard",
            "proposer": "0xProposer",
            "execute_event": {"block_number": 300, "transaction_index": 0},
        }}
        mock_http.get = AsyncMock(side_effect=[progress_resp, proposals_resp, detail_resp])

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])
        mock_gcs_client.read_ndjson = AsyncMock(return_value=None)

        mock_bc.get_blocktime = AsyncMock(return_value=past)
        mock_conn.fetch = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "EXECUTED"
        metadata = mock_gcs_client.upload_dict.call_args_list[0][1]["metadata"]
        assert metadata["liveness"] == "archived"


# ── Hybrid proposal handling ────────────────────────────────────────

class TestHybridProposals:

    @pytest.mark.asyncio
    async def test_hybrid_proposal_merges_govless(self, daonode_sync, mock_gcs_client):
        """Optimism hybrid proposals merge govless votes and hasnt_voted."""
        sync, mock_http, mock_bc, _, mock_conn = daonode_sync

        now = int(time.time())
        past = now - 7200

        progress_resp = MagicMock()
        progress_resp.json.return_value = {"block": 500}
        proposals_resp = MagicMock()
        proposals_resp.json.return_value = {"proposals": [{"id": "33333"}]}
        detail_resp = MagicMock()
        detail_resp.json.return_value = {"proposal": {
            "id": "33333", "description": "# Hybrid Prop",
            "start_block": 100, "end_block": 200,
            "block_number": 90, "transaction_index": 0,
            "voting_module_name": "standard",
            "proposer": "0xProposer",
        }}
        mock_http.get = AsyncMock(side_effect=[progress_resp, proposals_resp, detail_resp])

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])
        mock_gcs_client.read_ndjson = AsyncMock(return_value=None)

        # read_dict for govless proposal returns data
        mock_gcs_client.read_dict = AsyncMock(return_value={
            "title": "Govless Title", "description": "Govless Desc",
            "outcome": {"some": "data"}
        })

        mock_bc.get_blocktime = AsyncMock(return_value=past)

        # Set up govless mapping and other DB queries
        fetch_count = [0]
        async def multi_fetch(qry=None, *args, **kwargs):
            fetch_count[0] += 1
            qry_str = str(qry)
            if "offchain_proposals" in qry_str:
                return [{"govless_proposal_id": "govless-1", "governor_proposalid": "33333"}]
            if "delegate_statements" in qry_str:
                return []
            if "token_delegate_votes_changed" in qry_str:
                return []
            # read_votes_from_db
            return []
        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["hybrid"] is True


# ── get_transaction_hash ────────────────────────────────────────────

class TestGetTransactionHash:

    @pytest.mark.asyncio
    async def test_success(self, daonode_sync):
        sync, _, mock_bc, _, _ = daonode_sync
        mock_bc.get_transaction_by_index = AsyncMock(return_value={"tx": "0xabc"})
        result = await sync.get_transaction_hash(10, 100, 0)
        assert result == "0xabc"

    @pytest.mark.asyncio
    async def test_failure_returns_none(self, daonode_sync):
        sync, _, mock_bc, _, _ = daonode_sync
        mock_bc.get_transaction_by_index = AsyncMock(side_effect=Exception("timeout"))
        result = await sync.get_transaction_hash(10, 100, 0)
        assert result is None
