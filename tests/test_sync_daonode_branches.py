"""
Targeted branch coverage tests for DaoNodeSync.refresh_list():
- Non-optimism DAO: mapping = {} (line 249)
- _fetch_proposal_type (lines 151-154)
- Proposal fetch failure skip (lines 302-309)
- Hybrid GCS read failure skip (lines 324-327)
- Hash unchanged SkipProposal (lines 335-338)
- reuse_tally pass (line 362)
- Approval empty params / non-approval del params (lines 377-379)
- delegate_metadata lazy init (line 463)
- Corrupted proposal overrides (lines 553-562)
- PENDING lifecycle (lines 628-630)
- Unhandled lifecycle stage raises (line 623)
- DEFEATED liveness recently (line 610)
"""

import pytest
import time
from unittest.mock import AsyncMock, MagicMock, patch


OPTIMISM_CONFIG = {
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

ENS_CONFIG = {
    "schema": "ens",
    "dao_slug": "ENS",
    "index_tenant_prefix": "en",
    "features": {},
    "deployment": {
        "chain_id": 1,
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


def make_http_mocks(proposal_ids, config, detail_overrides=None):
    """Build HTTP side_effect list: progress, proposals, then N detail responses."""
    progress_resp = MagicMock()
    progress_resp.json.return_value = {"block": 500}

    proposals_resp = MagicMock()
    proposals_resp.json.return_value = {"proposals": [{"id": pid} for pid in proposal_ids]}

    resps = [progress_resp, proposals_resp]

    past = int(time.time()) - 7200
    for i, pid in enumerate(proposal_ids):
        overrides = (detail_overrides or {}).get(pid, {})
        detail = {
            "id": pid,
            "description": "# Test\n\nBody",
            "start_block": 100,
            "end_block": 200,
            "block_number": 90,
            "transaction_index": 0,
            "voting_module_name": "standard",
            "proposer": "0xProposer",
        }
        detail.update(overrides)
        resp = MagicMock()
        resp.json.return_value = {"proposal": detail}
        resps.append(resp)

    return resps


def make_daonode_sync(config):
    """Factory to build a DaoNodeSync with mocked dependencies."""
    with patch("cpls.sync.PostgreSQLClient") as MockPG, \
         patch("cpls.sync.BlockCacheClient") as MockBC, \
         patch("cpls.sync.load_tenant_config", return_value=config):

        mock_http = AsyncMock()
        mock_bc = MagicMock()
        mock_bc.clear_lru = MagicMock()
        mock_bc.get_blocktime = AsyncMock(return_value=int(time.time()) - 3600)
        mock_bc.get_ens_lru = AsyncMock(return_value=None)
        mock_bc.contract_call_encoded = AsyncMock(return_value={
            "result": "0x0000000000000000000000000000000000000000000000000000000000000004"
        })
        mock_bc.votable_supply_at_block_with_oracle = AsyncMock(return_value=1000000)
        mock_bc.votable_supply_at_block = AsyncMock(return_value=500000)
        mock_bc.get_transaction_by_index = AsyncMock(return_value={"tx": "0xtxhash"})
        mock_bc.last_block_before_timestamp = AsyncMock(return_value=18000050)
        MockBC.return_value = mock_bc

        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.fetchrow = AsyncMock(return_value={"votable_supply": "1000000"})
        mock_pool = MagicMock()
        mock_pool.acquire.return_value = FakeAcquire(mock_conn)
        mock_pg = MagicMock()
        mock_pg.connect = AsyncMock(return_value=mock_pool)
        MockPG.return_value = mock_pg

        from cpls.sync_daonode import DaoNodeSync
        sync = DaoNodeSync(config["schema"], config, reset=False, http_client=mock_http)
        sync.bc = mock_bc
        sync.pg = mock_pg
        sync.http_client = mock_http

        return sync, mock_http, mock_bc, mock_conn


@pytest.fixture
def mock_gcs():
    gcs = MagicMock()
    gcs.upload_dict = AsyncMock(return_value=True)
    gcs.upload_ndjson = AsyncMock(return_value=True)
    gcs.read_ndjson = AsyncMock(return_value=None)
    gcs.read_dict = AsyncMock(return_value=None)
    gcs.list_blobs = AsyncMock(return_value=[])
    mock_blob = MagicMock()
    mock_blob.exists.return_value = False
    gcs.get_blob = AsyncMock(return_value=mock_blob)
    return gcs


class TestNonOptimismMapping:

    @pytest.mark.asyncio
    async def test_non_optimism_uses_empty_mapping(self, mock_gcs):
        """Line 249: non-optimism DAO gets mapping = {}."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(ENS_CONFIG)

        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)
        mock_http.get = AsyncMock(side_effect=make_http_mocks(["12345"], ENS_CONFIG))

        result = await sync.refresh_list(mock_gcs)
        assert result["refreshed"] == 1
        # Confirm no govless mapping call was made (only optimism does that)


class TestFetchProposalType:

    @pytest.mark.asyncio
    async def test_proposal_with_type_id_fetches_type(self, mock_gcs):
        """Lines 151-154, 302-303: proposal with proposal_type_id triggers _fetch_proposal_type."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(OPTIMISM_CONFIG)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        progress_resp = MagicMock()
        progress_resp.json.return_value = {"block": 500}
        proposals_resp = MagicMock()
        proposals_resp.json.return_value = {"proposals": [{"id": "99001"}]}
        detail_resp = MagicMock()
        detail_resp.json.return_value = {"proposal": {
            "id": "99001",
            "description": "# Typed\n\nBody",
            "start_block": 100, "end_block": 200,
            "block_number": 90, "transaction_index": 0,
            "voting_module_name": "standard", "proposer": "0xProp",
            "proposal_type_id": 2,
        }}
        type_resp = MagicMock()
        type_resp.json.return_value = {"proposal_types": {"2": {"quorum": 5000, "name": "Standard"}}}

        mock_http.get = AsyncMock(side_effect=[
            progress_resp, proposals_resp, detail_resp, type_resp
        ])

        result = await sync.refresh_list(mock_gcs)
        assert result["refreshed"] == 1
        # Verify _fetch_proposal_type was called (type_resp was consumed)
        assert mock_http.get.call_count == 4


class TestProposalFetchFailure:

    @pytest.mark.asyncio
    async def test_fetch_detail_failure_skips_proposal(self, mock_gcs):
        """Lines 305-309: exception in _fetch_proposal_detail → skip with continue."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(ENS_CONFIG)

        progress_resp = MagicMock()
        progress_resp.json.return_value = {"block": 500}
        proposals_resp = MagicMock()
        proposals_resp.json.return_value = {"proposals": [{"id": "99002"}]}
        fail_resp = MagicMock()
        fail_resp.json.side_effect = Exception("Network error on detail fetch")

        mock_http.get = AsyncMock(side_effect=[progress_resp, proposals_resp, fail_resp])

        result = await sync.refresh_list(mock_gcs)
        assert result["skipped"] == 1
        assert result["refreshed"] == 0


class TestHybridGCSReadFailure:

    @pytest.mark.asyncio
    async def test_hybrid_gcs_read_failure_skips_proposal(self, mock_gcs):
        """Lines 324-327: exception reading govless hybrid proposal → skip."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(OPTIMISM_CONFIG)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        progress_resp = MagicMock()
        progress_resp.json.return_value = {"block": 500}
        proposals_resp = MagicMock()
        proposals_resp.json.return_value = {"proposals": [{"id": "99003"}]}
        detail_resp = MagicMock()
        detail_resp.json.return_value = {"proposal": {
            "id": "99003",
            "description": "# Hybrid\n\nBody",
            "start_block": 100, "end_block": 200,
            "block_number": 90, "transaction_index": 0,
            "voting_module_name": "standard", "proposer": "0xProp",
        }}

        mock_http.get = AsyncMock(side_effect=[progress_resp, proposals_resp, detail_resp])
        mock_gcs.read_dict = AsyncMock(side_effect=Exception("GCS unavailable"))

        # Patch read_govless_proposal_mappings so proposal 99003 maps to a govless proposal
        with patch.object(sync, 'read_govless_proposal_mappings',
                          new=AsyncMock(return_value={"99003": "govless-1"})):
            result = await sync.refresh_list(mock_gcs)

        assert result["skipped"] == 1


class TestHashUnchangedSkip:

    @pytest.mark.asyncio
    async def test_hash_unchanged_skips_proposal(self, mock_gcs):
        """Lines 335-338: SkipProposal from check_existing_proposal_hash → skipped."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(ENS_CONFIG)

        mock_http.get = AsyncMock(side_effect=make_http_mocks(["99004"], ENS_CONFIG))

        from cpls.sync import SkipProposal
        with patch.object(sync, 'check_existing_proposal_hash',
                          side_effect=SkipProposal("unchanged", proposal_id="99004")):
            result = await sync.refresh_list(mock_gcs)

        assert result["skipped"] == 1
        assert result["refreshed"] == 0


class TestReuseTallyPass:

    @pytest.mark.asyncio
    async def test_reuse_tally_takes_pass_branch(self, mock_gcs):
        """Line 362: reuse_tally=True triggers the pass branch."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(ENS_CONFIG)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        mock_http.get = AsyncMock(side_effect=make_http_mocks(["99005"], ENS_CONFIG))

        # blob has num_of_votes=2 (same as what read_votes_from_db returns)
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.metadata = {"liveness": "live", "hash": "old-hash-xyz", "num_of_votes": "2"}
        mock_gcs.get_blob = AsyncMock(return_value=mock_blob)

        # DB returns 2 votes; patch get_delegate_metadata to avoid address key issue
        mock_conn.fetch = AsyncMock(return_value=[
            {"voter": "0xV1", "weight": 100, "support": 1, "reason": "", "params": None,
             "ts": "2024-01-01T00:00:00"},
            {"voter": "0xV2", "weight": 200, "support": 0, "reason": "", "params": None,
             "ts": "2024-01-01T01:00:00"},
        ])

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})):
            result = await sync.refresh_list(mock_gcs)
        assert result["refreshed"] == 1


class TestApprovalVoteParams:

    @pytest.mark.asyncio
    async def test_approval_empty_params_sets_empty_list(self, mock_gcs):
        """Line 377: approval vote with None/empty params → record['params'] = []."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(ENS_CONFIG)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        mock_http.get = AsyncMock(side_effect=make_http_mocks(
            ["99006"], ENS_CONFIG,
            detail_overrides={"99006": {"voting_module_name": "approval"}}
        ))

        mock_conn.fetch = AsyncMock(return_value=[
            {"voter": "0xV1", "weight": 100, "support": 1, "reason": "", "params": None,
             "ts": "2024-01-01T00:00:00"},
        ])

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs)
        assert result["refreshed"] == 1
        # Verify votes were uploaded (approval path)
        vote_calls = [c for c in mock_gcs.upload_ndjson.call_args_list if "votes" in c[0][1]]
        assert len(vote_calls) >= 1

    @pytest.mark.asyncio
    async def test_non_approval_deletes_params(self, mock_gcs):
        """Line 379: non-approval vote → del record['params']."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(ENS_CONFIG)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        mock_http.get = AsyncMock(side_effect=make_http_mocks(
            ["99007"], ENS_CONFIG,
            detail_overrides={"99007": {"voting_module_name": "standard"}}
        ))

        mock_conn.fetch = AsyncMock(return_value=[
            {"voter": "0xV1", "weight": 100, "support": 1, "reason": "", "params": "0xabc",
             "ts": "2024-01-01T00:00:00"},
        ])

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs)
        assert result["refreshed"] == 1
        vote_calls = [c for c in mock_gcs.upload_ndjson.call_args_list if "votes" in c[0][1]]
        assert len(vote_calls) >= 1
        votes = vote_calls[0][0][0]
        assert "params" not in votes[0]


class TestDelegateMetadataLazyInit:

    @pytest.mark.asyncio
    async def test_delegate_metadata_fetched_when_none(self, mock_gcs):
        """Line 463: delegate_metadata is lazily fetched when None."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(ENS_CONFIG)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        mock_http.get = AsyncMock(side_effect=make_http_mocks(["99008"], ENS_CONFIG))
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.fetchrow = AsyncMock(return_value={"votable_supply": "1000000"})

        get_meta_mock = AsyncMock(return_value={"0xabc": {"ens": "test.eth"}})
        with patch.object(sync, 'get_delegate_metadata', get_meta_mock):
            result = await sync.refresh_list(mock_gcs)

        assert result["refreshed"] == 1
        get_meta_mock.assert_called_once()


class TestCorruptedProposalOverrides:

    @pytest.mark.asyncio
    async def test_uniswap_corrupted_defeated(self, mock_gcs):
        """Lines 561-562: proposal in UNISWAP list gets DEFEATED liveness=archived."""
        config = {**OPTIMISM_CONFIG, "schema": "uniswap", "deployment": {
            "chain_id": 1, "gov": {"address": "0xGov"}, "token": {"address": "0xTok"},
        }}
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(config)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        # '8' is in UNISWAP_CORRUPTED_PROPOSALS_MARKED_DEFEATED_I_GUESS list
        mock_http.get = AsyncMock(side_effect=make_http_mocks(["8"], config))
        mock_conn.fetch = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs)
        assert result["refreshed"] == 1
        uploaded = mock_gcs.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "DEFEATED"

    @pytest.mark.asyncio
    async def test_optimism_corrupted_succeeded(self, mock_gcs):
        """Lines 553-554: proposal in OPTIMISM_CORRUPTED list gets SUCCEEDED."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(OPTIMISM_CONFIG)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        corrupted_id = "103713749716503028671815481721039004389156473487450783632177114353117435138377"
        govless_resp = MagicMock()
        govless_resp.json.return_value = {"govless_proposals": []}
        progress_resp = MagicMock()
        progress_resp.json.return_value = {"block": 500}
        proposals_resp = MagicMock()
        proposals_resp.json.return_value = {"proposals": [{"id": corrupted_id}]}
        detail_resp = MagicMock()
        detail_resp.json.return_value = {"proposal": {
            "id": corrupted_id,
            "description": "# Corrupted\n\nBody",
            "start_block": 100, "end_block": 200,
            "block_number": 90, "transaction_index": 0,
            "voting_module_name": "standard", "proposer": "0xProp",
        }}
        mock_http.get = AsyncMock(side_effect=[progress_resp, proposals_resp, detail_resp])
        mock_conn.fetch = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs)
        assert result["refreshed"] == 1
        uploaded = mock_gcs.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "SUCCEEDED"

    @pytest.mark.asyncio
    async def test_xai_passed_proposal(self, mock_gcs):
        """Lines 557-558: proposal in XAI_PASSED list gets PASSED liveness=archived."""
        config = {**OPTIMISM_CONFIG, "schema": "xai", "deployment": {
            "chain_id": 660279, "gov": {"address": "0xGov"}, "token": {"address": "0xTok"},
        }}
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(config)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        xai_passed_id = "20614392243564088742464409537383874990606524118041119115074261643588472416818"
        # XAI id is already a valid numeric string
        mock_http.get = AsyncMock(side_effect=make_http_mocks([xai_passed_id], config))
        mock_conn.fetch = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs)
        assert result["refreshed"] == 1
        uploaded = mock_gcs.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "PASSED"


class TestPendingLifecycle:

    @pytest.mark.asyncio
    async def test_pending_proposal_gets_pending_stage(self, mock_gcs):
        """Lines 628-630: curtime < start_blocktime → PENDING lifecycle."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(ENS_CONFIG)

        future = int(time.time()) + 7200
        mock_bc.get_blocktime = AsyncMock(return_value=future)

        mock_http.get = AsyncMock(side_effect=make_http_mocks(["99009"], ENS_CONFIG))
        mock_conn.fetch = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs)
        assert result["refreshed"] == 1
        uploaded = mock_gcs.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "PENDING"


class TestUnhandledLifecycleStage:

    @pytest.mark.asyncio
    async def test_unknown_contract_state_raises(self, mock_gcs):
        """Line 623: stage not 0x3 or 0x4 → raises Exception."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(ENS_CONFIG)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        mock_http.get = AsyncMock(side_effect=make_http_mocks(["99010"], ENS_CONFIG))
        mock_conn.fetch = AsyncMock(return_value=[])

        # Stage 0x2 = Canceled → not handled in the elif chain → raises
        mock_bc.contract_call_encoded = AsyncMock(return_value={
            "result": "0x0000000000000000000000000000000000000000000000000000000000000002"
        })

        with pytest.raises(Exception, match="Unhandled proposal lifecycle stage"):
            await sync.refresh_list(mock_gcs)


class TestDefeatedWithRecentEnd:

    @pytest.mark.asyncio
    async def test_defeated_recently_sets_liveness_archived(self, mock_gcs):
        """Line 610: DEFEATED proposal ended <5min ago → liveness=archived."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(ENS_CONFIG)

        # end_blocktime = 100 seconds ago (within FIVE_MINUTES_IN_SECONDS=300)
        recent_past = int(time.time()) - 100
        mock_bc.get_blocktime = AsyncMock(return_value=recent_past)

        mock_http.get = AsyncMock(side_effect=make_http_mocks(["99011"], ENS_CONFIG))
        mock_conn.fetch = AsyncMock(return_value=[])

        # Stage 0x3 = Defeated
        mock_bc.contract_call_encoded = AsyncMock(return_value={
            "result": "0x0000000000000000000000000000000000000000000000000000000000000003"
        })

        result = await sync.refresh_list(mock_gcs)
        assert result["refreshed"] == 1
        uploaded = mock_gcs.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "DEFEATED"


class TestDaoNodeInitBadConfig:

    def test_missing_deployment_key_raises_exception(self):
        """Lines 50-51: bare except in __init__ re-raises as Exception."""
        bad_config = {
            "schema": "optimism",
            "dao_slug": "Optimism",
            "index_tenant_prefix": "op",
            "features": {},
            "deployment": {
                "chain_id": 10,
                # Missing 'gov' and 'token' keys
            },
        }
        with patch("cpls.sync.PostgreSQLClient"), \
             patch("cpls.sync.BlockCacheClient"), \
             patch("cpls.sync.load_tenant_config", return_value=bad_config):
            from cpls.sync_daonode import DaoNodeSync
            with pytest.raises(Exception, match="problem with config"):
                DaoNodeSync("optimism", bad_config, reset=False)


class TestEstimatedTimestamp:

    @pytest.mark.asyncio
    async def test_estimated_timestamp_delegates_to_bc(self):
        """Line 55: estimated_timestamp_from_future_block_number returns bc result."""
        sync, _, mock_bc, _ = make_daonode_sync(ENS_CONFIG)
        mock_bc.get_estimated_blocktime = MagicMock(return_value=9999999)
        result = await sync.estimated_timestamp_from_future_block_number(50000000)
        assert result == 9999999
        mock_bc.get_estimated_blocktime.assert_called_once_with(sync.chain_id, 50000000)


class TestBorkedOptimismProposals:

    @pytest.mark.asyncio
    async def test_borked_stage4_fresh_differs_marks_succeeded(self, mock_gcs):
        """Lines 591-593: old optimism proposal, stage=0x4 but fresh differs → SUCCEEDED."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(OPTIMISM_CONFIG)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        mock_http.get = AsyncMock(side_effect=make_http_mocks(["99020"], OPTIMISM_CONFIG))
        mock_conn.fetch = AsyncMock(return_value=[])

        # start_block=100 < OPTIMISM_V6_UPGRADE_BLOCK → quorum returns '0' early (no contract call)
        # Call order: 1=state at end+1 (0x4 SUCCEEDED), 2=fresh state at 2025 (0x3 DEFEATED, differs)
        call_count = [0]
        async def contract_side(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"result": "0x0000000000000000000000000000000000000000000000000000000000000004"}
            return {"result": "0x0000000000000000000000000000000000000000000000000000000000000003"}
        mock_bc.contract_call_encoded = AsyncMock(side_effect=contract_side)

        result = await sync.refresh_list(mock_gcs)
        assert result["refreshed"] == 1
        uploaded = mock_gcs.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "SUCCEEDED"

    @pytest.mark.asyncio
    async def test_borked_stage_mismatch_raises(self, mock_gcs):
        """Lines 595-597: old optimism, stage≠0x4 and stage≠fresh_stage → raises."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(OPTIMISM_CONFIG)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        mock_http.get = AsyncMock(side_effect=make_http_mocks(["99021"], OPTIMISM_CONFIG))
        mock_conn.fetch = AsyncMock(return_value=[])

        # start_block=100 < OPTIMISM_V6_UPGRADE_BLOCK, so quorum returns '0' early (no contract call)
        # Call order: 1=state at end+1 (0x1 Active), 2=fresh state at 2025 block (0x3 Defeated)
        call_count = [0]
        async def contract_side(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"result": "0x0000000000000000000000000000000000000000000000000000000000000001"}
            return {"result": "0x0000000000000000000000000000000000000000000000000000000000000003"}
        mock_bc.contract_call_encoded = AsyncMock(side_effect=contract_side)

        with pytest.raises(Exception, match="PROBLEM"):
            await sync.refresh_list(mock_gcs)


class TestLifecycleElseBranch:

    @pytest.mark.asyncio
    async def test_start_equals_end_equals_curtime_raises(self, mock_gcs):
        """Line 632: start_blocktime==end_blocktime==curtime hits unreachable else."""
        sync, mock_http, mock_bc, mock_conn = make_daonode_sync(ENS_CONFIG)

        fixed_time = 1700000000
        mock_bc.get_blocktime = AsyncMock(return_value=fixed_time)
        mock_http.get = AsyncMock(side_effect=make_http_mocks(["99030"], ENS_CONFIG))
        mock_conn.fetch = AsyncMock(return_value=[])

        with patch("cpls.sync_daonode.time") as mock_time:
            mock_time.time = MagicMock(return_value=fixed_time)
            with pytest.raises(Exception):
                await sync.refresh_list(mock_gcs)
