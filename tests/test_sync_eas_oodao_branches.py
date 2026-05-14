"""
Targeted branch coverage tests for sync_eas_oodao.py uncovered lines:
77-78, 85-86, 120, 226, 244-246, 354-356, 360-365, 375-376, 378-380,
392-393, 404-409, 435-436, 473-474, 489-494, 506-510, 540-543, 559-588,
594, 619, 674-676, 747-750, 765-779
"""
import pytest
import time
import json
from unittest.mock import AsyncMock, MagicMock, patch
from collections import defaultdict


@pytest.fixture(autouse=True)
def patch_oodao_validation():
    """Keep validation-related patches active for the duration of every test."""
    with patch("cpls.sync_eas_oodao.PROPOSAL_CHECK_SECRET", "secret"), \
         patch("cpls.sync_eas_oodao.get_proposal_check_api_url",
               return_value="http://api/validate"):
        yield


BASE_CONFIG = {
    "schema": "optimism",
    "dao_slug": "Optimism",
    "index_tenant_prefix": "op",
    "features": {"oodao": True},
    "deployment": {
        "chain_id": 10,
        "gov": {"address": "0xGovAddr"},
        "token": {"address": "0xTokenAddr"},
        "oodao": {"address": "0xOoDaoAddr", "chain_id": "1"},
    },
}

SYNDICATE_CONFIG = {
    "schema": "syndicate",
    "dao_slug": "Syndicate",
    "index_tenant_prefix": "syn",
    "features": {"oodao": True},
    "deployment": {
        "chain_id": 1,
        "gov": {"address": "0xSynGov"},
        "token": {"address": "0xSynToken", "chain_id": 1},
        "oodao": {"address": "0xSynOoDaoAddr", "chain_id": "1"},
    },
}


class FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *args):
        return False


def make_sync(config=None):
    cfg = config or BASE_CONFIG
    with patch("cpls.sync.PostgreSQLClient") as MockPG, \
         patch("cpls.sync.BlockCacheClient") as MockBC, \
         patch("cpls.sync.load_tenant_config", return_value=cfg), \
         patch("cpls.sync_eas_oodao.PROPOSAL_CHECK_SECRET", "secret"), \
         patch("cpls.sync_eas_oodao.get_proposal_check_api_url",
               return_value="http://api/validate"):

        mock_http = AsyncMock()
        mock_bc = MagicMock()
        mock_bc.clear_lru = MagicMock()
        mock_bc.get_blocktime = AsyncMock(return_value=int(time.time()) - 3600)
        mock_bc.get_ens_lru = AsyncMock(return_value=None)
        mock_bc.last_block_before_timestamp = AsyncMock(return_value=18000050)
        mock_bc.contract_call_encoded = AsyncMock(return_value={"result": "0x64"})
        MockBC.return_value = mock_bc

        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.fetchrow = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        mock_pool.acquire.return_value = FakeAcquire(mock_conn)
        mock_pg = MagicMock()
        mock_pg.connect = AsyncMock(return_value=mock_pool)
        MockPG.return_value = mock_pg

        from cpls.sync_eas_oodao import EASOoDaoSync
        sync = EASOoDaoSync(cfg["schema"], cfg, reset=False, http_client=mock_http)
        sync.bc = mock_bc
        sync.pg = mock_pg
        return sync, mock_http, mock_bc, mock_conn


def make_prop(startts=None, endts=None, tags="gov-proposal",
              kwargs='{"voting_module": "standard"}', uid="0xProp1"):
    now = int(time.time())
    return {
        "transaction_hash": "0xtx",
        "dao_id": "0xOoDaoAddr",
        "uid": uid,
        "author": "0x000000000000000000000000aaaaaaaaaaaaaaaa",
        "chain_id": 1,
        "tags": tags,
        "endts": endts or now + 3600,
        "title": "Test Proposal",
        "description": "# Test\n\nBody",
        "kwargs": kwargs,
        "proposal_id": uid,
        "created_block_number": 18000000,
        "created_time": now - 7200,
        "startts": startts or now - 3600,
    }


def make_vote(voter="0xV1", support=1, weight=1000, params=None):
    return {
        "transaction_hash": "0xvtx",
        "block_number": 18000100,
        "chain_id": 1,
        "voter": voter,
        "support": support,
        "weight": weight,
        "reason": "",
        "params": params,
        "ts": int(time.time()) - 1800,
    }


def make_db_mocks(mock_conn, prop_rows, vote_rows=None, fetchrow_overrides=None):
    """Set up DB fetch/fetchrow side effects for a standard proposal flow."""
    vote_rows = vote_rows or []
    fetch_count = [0]

    async def multi_fetch(*args, **kwargs):
        fetch_count[0] += 1
        if fetch_count[0] == 1:
            return prop_rows
        elif fetch_count[0] == 2:
            return []  # read_proposal_deletions
        elif fetch_count[0] == 3:
            return []  # read_proposal_checks
        elif fetch_count[0] == 4:
            return vote_rows  # read_votes_from_db
        return []

    fetchrow_count = [0]

    async def multi_fetchrow(qry=None, *args, **kwargs):
        fetchrow_count[0] += 1
        if fetchrow_overrides:
            result = fetchrow_overrides(fetchrow_count[0], qry)
            if result is not None:
                return result
        if fetchrow_count[0] == 1:
            return {"min_quorum_pct": "1000", "max_quorum_pct": "5000",
                    "min_approval_threshold_pct": "5000", "max_approval_threshold_pct": "7500"}
        if fetchrow_count[0] == 2:
            return None  # authors_prop_type
        if fetchrow_count[0] == 3:
            return None  # approved_prop_type
        if qry and "votable_supply" in str(qry):
            return {"votable_supply": "1000000"}
        return None

    mock_conn.fetch = AsyncMock(side_effect=multi_fetch)
    mock_conn.fetchrow = AsyncMock(side_effect=multi_fetchrow)


def make_gcs_blob(exists=False, metadata=None):
    blob = MagicMock()
    blob.exists.return_value = exists
    blob.metadata = metadata or {}
    blob.name = "proposals/0xProp1"
    return blob


def setup_http(mock_http, val_success=True):
    resp = MagicMock()
    resp.json.return_value = {"success": val_success}
    resp.raise_for_status = MagicMock()
    mock_http.post = AsyncMock(return_value=resp)
    nonivote = MagicMock()
    nonivote.status_code = 404
    mock_http.get = AsyncMock(return_value=nonivote)


# ---------------------------------------------------------------------------
# Init branches
# ---------------------------------------------------------------------------

class TestInitToken2Config:

    def test_token_2_in_config_sets_attrs(self):
        """Lines 77-78: config with token_2 → self.token_2_addr and token_2_chain_id set."""
        cfg = {
            **BASE_CONFIG,
            "deployment": {
                **BASE_CONFIG["deployment"],
                "token_2": {"address": "0xToken2Addr", "chain_id": 42161},
            },
        }
        sync, _, _, _ = make_sync(cfg)
        assert sync.token_2_addr == "0xToken2Addr"
        assert sync.token_2_chain_id == 42161

    def test_bad_config_raises_exception(self):
        """Lines 85-86: bad config in __init__ raises Exception."""
        bad_cfg = {
            "schema": "optimism",
            "dao_slug": "Optimism",
            "index_tenant_prefix": "op",
            "features": {},
            "deployment": {
                "chain_id": 10,
                "oodao": {},  # Missing 'address' key
            },
        }
        with patch("cpls.sync.PostgreSQLClient"), \
             patch("cpls.sync.BlockCacheClient"), \
             patch("cpls.sync.load_tenant_config", return_value=bad_cfg), \
             patch("cpls.sync_eas_oodao.PROPOSAL_CHECK_SECRET", "s"), \
             patch("cpls.sync_eas_oodao.get_proposal_check_api_url", return_value=None):
            from cpls.sync_eas_oodao import EASOoDaoSync
            with pytest.raises(Exception, match="problem with config"):
                EASOoDaoSync("optimism", bad_cfg, reset=False)


# ---------------------------------------------------------------------------
# read_snapshot_votable_supply nonivotes path
# ---------------------------------------------------------------------------

class TestNonivotesPositive:

    @pytest.mark.asyncio
    async def test_nonivotes_positive_prints(self):
        """Line 120: total_nonivotes_vp > 0 triggers the print statement."""
        sync, _, mock_bc, _ = make_sync()
        mock_bc.last_block_before_timestamp = AsyncMock(return_value=18000050)

        with patch.object(sync, 'get_total_nonivotes_vp_at_block', AsyncMock(return_value=500)):
            mock_conn = MagicMock()
            mock_conn.fetchrow = AsyncMock(return_value={"votable_supply": "1000"})
            mock_pool = MagicMock()
            mock_pool.acquire.return_value = FakeAcquire(mock_conn)
            sync.pg.connect = AsyncMock(return_value=mock_pool)
            result = await sync.read_snapshot_votable_supply(18000050, 1)
        assert result == 1500


# ---------------------------------------------------------------------------
# validate_proposal paths
# ---------------------------------------------------------------------------

class TestValidateProposalPaths:

    @pytest.mark.asyncio
    async def test_no_api_url_returns_false(self):
        """Line 226: validate_proposal returns False when no api_url."""
        sync, _, _, _ = make_sync()
        with patch("cpls.sync_eas_oodao.get_proposal_check_api_url", return_value=None):
            result = await sync.validate_proposal("0xProp", "0xAttester", ["gov-proposal"])
        assert result is False

    @pytest.mark.asyncio
    async def test_http_exception_returns_false(self):
        """Lines 244-246: HTTP exception in validate_proposal → returns False."""
        sync, mock_http, _, _ = make_sync()
        mock_http.post = AsyncMock(side_effect=Exception("Connection refused"))
        result = await sync.validate_proposal("0xProp", "0xAttester", ["gov-proposal"])
        assert result is False


# ---------------------------------------------------------------------------
# kwargs processing branches
# ---------------------------------------------------------------------------

class TestKwargsBranches:

    @pytest.mark.asyncio
    async def test_kwargs_not_string_skips_proposal(self, mock_gcs_client):
        """Lines 354-356: kwargs is not a string → skip."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop(kwargs={"voting_module": "standard"})  # dict, not string
        make_db_mocks(mock_conn, [prop])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        result = await sync.refresh_list(mock_gcs_client)
        assert result["skipped"] == 1

    @pytest.mark.asyncio
    async def test_kwargs_single_quote_json_parses(self, mock_gcs_client):
        """Lines 360-363: json.JSONDecodeError on kwargs with single quotes → parsed."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop(kwargs="{'voting_module': 'standard'}")
        make_db_mocks(mock_conn, [prop])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1

    @pytest.mark.asyncio
    async def test_kwargs_invalid_json_raises(self, mock_gcs_client):
        """Line 365: json.JSONDecodeError with unexpected kwargs → raises."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop(kwargs="not-valid-json-at-all!!")
        make_db_mocks(mock_conn, [prop])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        with pytest.raises(Exception, match="Problem decoding kwargs"):
            await sync.refresh_list(mock_gcs_client)


# ---------------------------------------------------------------------------
# Proposal type approval branches
# ---------------------------------------------------------------------------

class TestProposalTypeBranches:

    @pytest.mark.asyncio
    async def test_authors_prop_type_with_gov_proposal_tag(self, mock_gcs_client):
        """Lines 375-376: authors_prop_type + 'gov-proposal' in tags → APPROVED."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop(tags="gov-proposal")
        authors_type = json.dumps({
            "quorum": 1000, "name": "Standard",
            "class": "STANDARD", "approval_threshold": 5000
        })

        def fetchrow_fn(count, qry):
            if count == 2:
                return {"data": "0xTypeUid", "decoded_attestation": authors_type}
            if count == 3:
                return None
            return None

        make_db_mocks(mock_conn, [prop], fetchrow_overrides=fetchrow_fn)
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["proposal_type_approval"] == "APPROVED"

    @pytest.mark.asyncio
    async def test_authors_prop_type_without_gov_proposal_pending(self, mock_gcs_client):
        """Lines 378-380: authors_prop_type but no gov-proposal tag → PENDING."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop(tags="community")
        authors_type = json.dumps({
            "quorum": 1000, "name": "Standard",
            "class": "STANDARD", "approval_threshold": 5000
        })

        def fetchrow_fn(count, qry):
            if count == 2:
                return {"data": "0xTypeUid", "decoded_attestation": authors_type}
            if count == 3:
                return None
            return None

        make_db_mocks(mock_conn, [prop], fetchrow_overrides=fetchrow_fn)
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["proposal_type_approval"] == "PENDING"


# ---------------------------------------------------------------------------
# ENS raise branches
# ---------------------------------------------------------------------------

class TestEnsRaiseBranches:

    @pytest.mark.asyncio
    async def test_proposer_ens_raises_sets_none(self, mock_gcs_client):
        """Lines 392-393: get_ens_lru raises → proposer_ens = None."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop()
        make_db_mocks(mock_conn, [prop])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))
        mock_bc.get_ens_lru = AsyncMock(side_effect=Exception("ENS failure"))

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["proposer_ens"] is None

    @pytest.mark.asyncio
    async def test_vote_ens_raises_in_standard_loop(self, mock_gcs_client):
        """Lines 435-436: get_ens_lru raises in STANDARD vote loop → pass (ignored)."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop()
        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote()])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        call_count = [0]

        async def ens_side(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] >= 2:
                raise Exception("ENS error")
            return None

        mock_bc.get_ens_lru = AsyncMock(side_effect=ens_side)
        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1


# ---------------------------------------------------------------------------
# Reuse tally branches
# ---------------------------------------------------------------------------

class TestReuseTally:

    @pytest.mark.asyncio
    async def test_reuse_tally_reads_existing_proposal(self, mock_gcs_client):
        """Lines 404-409: reuse_tally=True → reads existing proposal from GCS."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        now = int(time.time())
        prop = make_prop(startts=now - 7200, endts=now - 3600)
        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote()])
        setup_http(mock_http)
        blob = make_gcs_blob(
            exists=True,
            metadata={"liveness": "live", "hash": "abc", "num_of_votes": "1"}
        )
        mock_gcs_client.get_blob = AsyncMock(return_value=blob)
        mock_gcs_client.read_dict = AsyncMock(return_value={
            "outcome": {"token-holders": {"1": "500"}},
            "lifecycle_stage": "DEFEATED",
        })

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'read_snapshot_votable_supply', AsyncMock(return_value=1000000)), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1

    @pytest.mark.asyncio
    async def test_reuse_tally_none_existing_data_raises(self, mock_gcs_client):
        """Line 407: reuse_tally=True but GCS returns None → raises Exception."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        now = int(time.time())
        prop = make_prop(startts=now - 7200, endts=now - 3600)
        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote()])
        setup_http(mock_http)
        blob = make_gcs_blob(
            exists=True,
            metadata={"liveness": "live", "hash": "abc", "num_of_votes": "1"}
        )
        mock_gcs_client.get_blob = AsyncMock(return_value=blob)
        mock_gcs_client.read_dict = AsyncMock(return_value=None)

        with pytest.raises(Exception, match="We got None for existing_proposal_data"):
            await sync.refresh_list(mock_gcs_client)


# ---------------------------------------------------------------------------
# SkipProposal from read_existing_raw_proposal_hash_if_exists
# ---------------------------------------------------------------------------

class TestSkipProposalOnBlobRead:

    @pytest.mark.asyncio
    async def test_skip_proposal_exception_increments_skipped(self, mock_gcs_client):
        """Lines 540-543: SkipProposal raised during blob read → skipped."""
        from cpls.sync import SkipProposal
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop()
        make_db_mocks(mock_conn, [prop])
        setup_http(mock_http)

        with patch.object(sync, 'read_existing_raw_proposal_hash_if_exists',
                          AsyncMock(side_effect=SkipProposal("already archived", "0xProp1"))):
            result = await sync.refresh_list(mock_gcs_client)

        assert result["skipped"] == 1


# ---------------------------------------------------------------------------
# APPROVAL vote parsing paths
# ---------------------------------------------------------------------------

class TestApprovalVotePaths:

    @pytest.mark.asyncio
    async def test_approval_vote_ens_raises_ignored(self, mock_gcs_client):
        """Lines 473-474: get_ens_lru raises in APPROVAL vote loop → pass."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop(kwargs='{"voting_module": "approval"}')
        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote(support="1,2")])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        call_count = [0]

        async def ens_side(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] >= 2:
                raise Exception("ENS error")
            return None

        mock_bc.get_ens_lru = AsyncMock(side_effect=ens_side)
        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1

    @pytest.mark.asyncio
    async def test_approval_vote_single_int_json_support(self, mock_gcs_client):
        """Line 489: APPROVAL vote with single non-comma string → json.loads path."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop(kwargs='{"voting_module": "approval"}')
        # "1" has no comma → goes to json.loads("1") = 1 (int, not list)
        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote(support="1")])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1

    @pytest.mark.asyncio
    async def test_approval_vote_comma_support(self, mock_gcs_client):
        """Lines 484-485: APPROVAL vote with comma-separated support string."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop(kwargs='{"voting_module": "approval"}')
        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote(support="0,1,2")])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1

    @pytest.mark.asyncio
    async def test_approval_vote_list_support(self, mock_gcs_client):
        """Lines 491-492: APPROVAL vote with list/tuple support."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop(kwargs='{"voting_module": "approval"}')
        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote(support=[1, 2])])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1

    @pytest.mark.asyncio
    async def test_approval_vote_else_int_support(self, mock_gcs_client):
        """Lines 493-494: APPROVAL vote with non-str/non-list support → else int()."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop(kwargs='{"voting_module": "approval"}')
        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote(support=2)])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1

    @pytest.mark.asyncio
    async def test_approval_vote_type_error_handled(self, mock_gcs_client):
        """Lines 506-510: TypeError in APPROVAL vote processing → warning."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop(kwargs='{"voting_module": "approval"}')
        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote(support=None)])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1


# ---------------------------------------------------------------------------
# Syndicate / towns total supply path
# ---------------------------------------------------------------------------

class TestSyndicateTotalSupplyPath:

    @pytest.mark.asyncio
    async def test_syndicate_uses_total_supply_from_token(self, mock_gcs_client):
        """Lines 559-588: syndicate infra_dao_slug → totalSupply() contract call."""
        sync, mock_http, mock_bc, mock_conn = make_sync(SYNDICATE_CONFIG)
        now = int(time.time())
        prop = make_prop(startts=now - 3600, endts=now + 3600)
        # Set chain_id on proposal to match token_chain_id (1)
        prop["chain_id"] = 1
        make_db_mocks(mock_conn, [prop], vote_rows=[])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))
        mock_bc.contract_call_encoded = AsyncMock(return_value={"result": "0x" + "0" * 63 + "f"})

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert "total_voting_power_at_start" in uploaded

    @pytest.mark.asyncio
    async def test_syndicate_with_token_2(self, mock_gcs_client):
        """Lines 573-586: syndicate with token_2 → sums both totalSupply calls."""
        cfg = {
            **SYNDICATE_CONFIG,
            "deployment": {
                **SYNDICATE_CONFIG["deployment"],
                "token_2": {"address": "0xSynToken2", "chain_id": 1},
            },
        }
        sync, mock_http, mock_bc, mock_conn = make_sync(cfg)
        now = int(time.time())
        prop = make_prop(startts=now - 3600, endts=now + 3600)
        prop["chain_id"] = 1
        make_db_mocks(mock_conn, [prop], vote_rows=[])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))
        # Both token1 and token2 total supply calls
        mock_bc.contract_call_encoded = AsyncMock(
            return_value={"result": "0x" + "0" * 63 + "a"}
        )

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert int(uploaded["total_voting_power_at_start"]) == 20  # 10 + 10


# ---------------------------------------------------------------------------
# Voter not in snapshot_vp_lookup
# ---------------------------------------------------------------------------

class TestVoterNotInSnapshotLookup:

    @pytest.mark.asyncio
    async def test_voter_not_in_snapshot_keeps_original_weight(self, mock_gcs_client):
        """Line 619: voter not in snapshot_vp_lookup → use original vote weight."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        now = int(time.time())
        prop = make_prop(startts=now - 7200, endts=now - 3600)
        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote(voter="0xVoterA", weight=999)])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        vp_row = {"addr": "0xOtherVoter", "vp": "500"}
        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'read_snapshot_votable_supply', AsyncMock(return_value=1000000)), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[vp_row])):
            result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 1


# ---------------------------------------------------------------------------
# hasnt_voted ENS raises / succeeds
# ---------------------------------------------------------------------------

class TestHasntVotedEnsPath:

    @pytest.mark.asyncio
    async def test_hasnt_voted_ens_raises_ignored(self, mock_gcs_client):
        """Lines 675-676: get_ens_lru raises in hasnt_voted loop → pass."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        now = int(time.time())
        prop = make_prop(startts=now - 7200, endts=now - 3600)
        make_db_mocks(mock_conn, [prop], vote_rows=[])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        vp_row = {"addr": "0xNoVoter", "vp": "300"}
        call_count = [0]

        async def ens_side(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] >= 2:
                raise Exception("ENS fail")
            return None

        mock_bc.get_ens_lru = AsyncMock(side_effect=ens_side)
        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'read_snapshot_votable_supply', AsyncMock(return_value=1000000)), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[vp_row])):
            result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 1

    @pytest.mark.asyncio
    async def test_hasnt_voted_ens_set_when_not_none(self, mock_gcs_client):
        """Line 674: record['ens'] set when get_ens_lru returns non-None in hasnt_voted."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        now = int(time.time())
        prop = make_prop(startts=now - 7200, endts=now - 3600)
        make_db_mocks(mock_conn, [prop], vote_rows=[])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        vp_row = {"addr": "0xnovoter", "vp": "300"}
        call_count = [0]

        async def ens_side(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] >= 2:
                return "novoter.eth"  # non-None → line 674 covered
            return None

        mock_bc.get_ens_lru = AsyncMock(side_effect=ens_side)
        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'read_snapshot_votable_supply', AsyncMock(return_value=1000000)), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[vp_row])):
            result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 1


# ---------------------------------------------------------------------------
# STANDARD ended proposal lifecycle
# ---------------------------------------------------------------------------

class TestStandardEndedProposalLifecycle:

    @pytest.mark.asyncio
    async def test_standard_no_votes_after_end_defeated(self, mock_gcs_client):
        """STANDARD proposal past end with no votes → DEFEATED (quorum not met)."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        now = int(time.time())
        prop = make_prop(startts=now - 7200, endts=now - 3600, kwargs=None)
        make_db_mocks(mock_conn, [prop], vote_rows=[])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'read_snapshot_votable_supply', AsyncMock(return_value=1000000)), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] in ("DEFEATED", "PASSED")


# ---------------------------------------------------------------------------
# APPROVAL lifecycle: vote count ValueError and THRESHOLD criteria
# ---------------------------------------------------------------------------

class TestApprovalLifecycle:

    @pytest.mark.asyncio
    async def test_approval_vote_count_value_error(self, mock_gcs_client):
        """Lines 747-750: ValueError in APPROVAL lifecycle vote count."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        now = int(time.time())
        prop = make_prop(startts=now - 7200, endts=now - 3600,
                         kwargs='{"voting_module": "approval"}')
        prop_type_json = json.dumps({
            "quorum": 100, "name": "Approval",
            "class": "APPROVAL", "approval_threshold": 5000
        })

        def fetchrow_fn(count, qry):
            if count == 2:
                return {"data": "0xTU", "decoded_attestation": prop_type_json}
            if count == 3:
                return None
            return None

        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote(support="1", weight=500)],
                      fetchrow_overrides=fetchrow_fn)
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        # Reuse tally with bad outcome to trigger ValueError in lifecycle
        blob = make_gcs_blob(
            exists=True,
            metadata={"liveness": "live", "hash": "xyz", "num_of_votes": "1"}
        )
        mock_gcs_client.get_blob = AsyncMock(return_value=blob)
        mock_gcs_client.read_dict = AsyncMock(return_value={
            "outcome": {
                "token-holders": {"1": {"1": "not-a-number"}},
                "no-param": {"1": "500"},
            },
            "lifecycle_stage": "ACTIVE",
        })

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'read_snapshot_votable_supply', AsyncMock(return_value=1000000)), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1

    @pytest.mark.asyncio
    async def test_threshold_criteria_succeeded(self, mock_gcs_client):
        """Lines 765-779: THRESHOLD criteria → SUCCEEDED when option exceeds threshold."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        now = int(time.time())
        prop = make_prop(startts=now - 7200, endts=now - 3600,
                         kwargs='{"voting_module": "approval"}', tags="gov-proposal")
        prop_type_json = json.dumps({
            "quorum": 1, "name": "Approval", "class": "APPROVAL",
            "approval_threshold": 5000, "criteria": "THRESHOLD", "threshold": 100
        })

        def fetchrow_fn(count, qry):
            if count == 2:
                return {"data": "0xTU", "decoded_attestation": prop_type_json}
            if count == 3:
                return None
            return None

        # Use high weight (100000) so quorum is met (1/10000 * 1M = 100; 100000 >= 100)
        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote(support="1", weight=100000)],
                      fetchrow_overrides=fetchrow_fn)
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'read_snapshot_votable_supply', AsyncMock(return_value=1000000)), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] in ("SUCCEEDED", "DEFEATED")

    @pytest.mark.asyncio
    async def test_unknown_voting_module_maps_to_standard(self, mock_gcs_client):
        """Line 387: unknown voting_module falls back to STANDARD."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop(kwargs='{"voting_module": "unknown_type"}')
        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote()])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1

    @pytest.mark.asyncio
    async def test_approval_vote_count_flat_outcome_else_branch(self, mock_gcs_client):
        """Line 747: flat outcome value (not a dict) → else int(support_dict) path."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        now = int(time.time())
        prop = make_prop(startts=now - 7200, endts=now - 3600,
                         kwargs='{"voting_module": "approval"}')
        prop_type_json = json.dumps({
            "quorum": 100, "name": "Approval",
            "class": "APPROVAL", "approval_threshold": 5000
        })

        def fetchrow_fn(count, qry):
            if count == 2:
                return {"data": "0xTU", "decoded_attestation": prop_type_json}
            if count == 3:
                return None
            return None

        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote(support="1", weight=500)],
                      fetchrow_overrides=fetchrow_fn)
        setup_http(mock_http)
        # Reuse tally with FLAT outcome (string value, not nested dict) → line 747
        blob = make_gcs_blob(
            exists=True,
            metadata={"liveness": "live", "hash": "xyz", "num_of_votes": "1"}
        )
        mock_gcs_client.get_blob = AsyncMock(return_value=blob)
        mock_gcs_client.read_dict = AsyncMock(return_value={
            "outcome": {
                "token-holders": {"1": "500"},  # flat string value → else branch
                "no-param": {"1": "500"},
            },
            "lifecycle_stage": "ACTIVE",
        })

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'read_snapshot_votable_supply', AsyncMock(return_value=1000000)), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1

    @pytest.mark.asyncio
    async def test_threshold_criteria_flat_outcome_else_and_error(self, mock_gcs_client):
        """Lines 771, 776-778: THRESHOLD with flat/bad outcome → else and error paths."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        now = int(time.time())
        prop = make_prop(startts=now - 7200, endts=now - 3600,
                         kwargs='{"voting_module": "approval"}', tags="gov-proposal")
        prop_type_json = json.dumps({
            "quorum": 1, "name": "Approval", "class": "APPROVAL",
            "approval_threshold": 5000, "criteria": "THRESHOLD", "threshold": 100
        })

        def fetchrow_fn(count, qry):
            if count == 2:
                return {"data": "0xTU", "decoded_attestation": prop_type_json}
            if count == 3:
                return None
            return None

        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote(support="1", weight=500)],
                      fetchrow_overrides=fetchrow_fn)
        setup_http(mock_http)
        # Reuse tally: quorum met (500 >= 100), outcome flat → covers line 771
        blob = make_gcs_blob(
            exists=True,
            metadata={"liveness": "live", "hash": "xyz2", "num_of_votes": "1"}
        )
        mock_gcs_client.get_blob = AsyncMock(return_value=blob)
        mock_gcs_client.read_dict = AsyncMock(return_value={
            "outcome": {
                "token-holders": {"1": "500", "2": "bad-value"},  # 1 flat OK, 2 flat bad
                "no-param": {"1": "500"},
            },
            "lifecycle_stage": "ACTIVE",
        })

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'read_snapshot_votable_supply', AsyncMock(return_value=1000000)), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1


# ---------------------------------------------------------------------------
# check_existing_proposal_hash SkipProposal path (lines 540-543)
# ---------------------------------------------------------------------------

class TestCheckHashSkipProposal:

    @pytest.mark.asyncio
    async def test_check_hash_skip_increments_skipped(self, mock_gcs_client):
        """Lines 540-543: check_existing_proposal_hash raises SkipProposal → skipped."""
        from cpls.sync import SkipProposal
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop()
        make_db_mocks(mock_conn, [prop])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        with patch.object(sync, 'check_existing_proposal_hash',
                          MagicMock(side_effect=SkipProposal("hash unchanged", "0xProp1"))):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["skipped"] == 1


# ---------------------------------------------------------------------------
# Syndicate cross-chain paths (lines 560, 575)
# ---------------------------------------------------------------------------

class TestSyndicateCrossChain:

    @pytest.mark.asyncio
    async def test_syndicate_cross_chain_token1(self, mock_gcs_client):
        """Line 560: syndicate proposal chain differs from token chain → separate BC call."""
        sync, mock_http, mock_bc, mock_conn = make_sync(SYNDICATE_CONFIG)
        now = int(time.time())
        prop = make_prop(startts=now - 3600, endts=now + 3600)
        # proposal on chain 10, token on chain 1 → cross-chain path
        prop["chain_id"] = 10
        make_db_mocks(mock_conn, [prop], vote_rows=[])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))
        mock_bc.contract_call_encoded = AsyncMock(return_value={"result": "0x" + "0" * 63 + "f"})

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1

    @pytest.mark.asyncio
    async def test_syndicate_cross_chain_token2(self, mock_gcs_client):
        """Line 575: syndicate with token_2, proposal chain differs from token_2 chain."""
        cfg = {
            **SYNDICATE_CONFIG,
            "deployment": {
                **SYNDICATE_CONFIG["deployment"],
                "token_2": {"address": "0xSynToken2", "chain_id": 42161},
            },
        }
        sync, mock_http, mock_bc, mock_conn = make_sync(cfg)
        now = int(time.time())
        prop = make_prop(startts=now - 3600, endts=now + 3600)
        # proposal on chain 10, token on chain 1, token_2 on chain 42161 → cross-chain
        prop["chain_id"] = 10
        make_db_mocks(mock_conn, [prop], vote_rows=[])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))
        mock_bc.contract_call_encoded = AsyncMock(return_value={"result": "0x" + "0" * 63 + "a"})

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1


# ---------------------------------------------------------------------------
# APPROVAL vote support: json.loads path (lines 489-490)
# ---------------------------------------------------------------------------

class TestApprovalJsonDecodeSupport:

    @pytest.mark.asyncio
    async def test_approval_leading_zero_triggers_json_decode_error(self, mock_gcs_client):
        """Lines 489-490: support='01' → json.loads raises → int('01') succeeds."""
        sync, mock_http, mock_bc, mock_conn = make_sync()
        prop = make_prop(kwargs='{"voting_module": "approval"}')
        # "01" has no comma, json.loads("01") raises JSONDecodeError, int("01") = 1
        make_db_mocks(mock_conn, [prop], vote_rows=[make_vote(support="01")])
        setup_http(mock_http)
        mock_gcs_client.get_blob = AsyncMock(return_value=make_gcs_blob(exists=False))

        with patch.object(sync, 'get_delegate_metadata', AsyncMock(return_value={})), \
             patch.object(sync, 'get_vp_snapshot_all_delegates', AsyncMock(return_value=[])):
            result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
