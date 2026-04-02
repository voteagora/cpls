"""
Integration tests for EASOoDaoSync.refresh_list() with fully mocked externals.
Tests proposal reading, vote processing, lifecycle determination, and archival.
"""

import pytest
import time
import json
import copy
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock
from collections import defaultdict


SAMPLE_CONFIG = {
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


class FakeAcquire:
    def __init__(self, conn):
        self._conn = conn
    async def __aenter__(self):
        return self._conn
    async def __aexit__(self, *args):
        return False


def make_proposal_row(proposal_id="0xPropUid", dao_id="0xOoDaoAddr",
                      author="0x000000000000000000000000aaaaaaaaaaaaaaaa",
                      tags="gov-proposal", title="Test Proposal",
                      description="# Test\n\nBody", startts=None, endts=None,
                      kwargs=None):
    now = int(time.time())
    return {
        "transaction_hash": "0xtx1",
        "dao_id": dao_id,
        "uid": proposal_id,
        "author": author,
        "chain_id": 1,
        "tags": tags,
        "endts": endts or now + 3600,
        "title": title,
        "description": description,
        "kwargs": kwargs or '{"voting_module": "standard"}',
        "proposal_id": proposal_id,
        "created_block_number": 18000000,
        "created_time": now - 7200,
        "startts": startts or now - 3600,
    }


def make_vote_row(voter="0xVoter1", support=1, weight=1000):
    return {
        "transaction_hash": "0xvotetx",
        "block_number": 18000100,
        "chain_id": 1,
        "voter": voter,
        "support": support,
        "weight": weight,
        "reason": "",
        "params": None,
        "ts": int(time.time()) - 1800,
    }


@pytest.fixture
def mocked_oodao():
    with patch("cpls.sync.PostgreSQLClient") as MockPG, \
         patch("cpls.sync.BlockCacheClient") as MockBC, \
         patch("cpls.sync.load_tenant_config", return_value=SAMPLE_CONFIG), \
         patch("cpls.sync_eas_oodao.PROPOSAL_CHECK_SECRET", "test-secret"), \
         patch("cpls.sync_eas_oodao.get_proposal_check_api_url", return_value="http://check-api/validate"):

        mock_http = AsyncMock()
        mock_bc = MagicMock()
        mock_bc.clear_lru = MagicMock()
        mock_bc.get_blocktime = AsyncMock(return_value=int(time.time()) - 3600)
        mock_bc.get_ens_lru = AsyncMock(return_value=None)
        mock_bc.get_ens = AsyncMock(return_value=None)
        mock_bc.last_block_before_timestamp = AsyncMock(return_value=18000050)
        mock_bc.contract_call_encoded = AsyncMock(return_value={"result": "0x64"})
        MockBC.return_value = mock_bc

        mock_pg = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.fetchrow = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        mock_pool.acquire.return_value = FakeAcquire(mock_conn)
        mock_pg.connect = AsyncMock(return_value=mock_pool)
        MockPG.return_value = mock_pg

        from cpls.sync_eas_oodao import EASOoDaoSync
        sync = EASOoDaoSync("optimism", SAMPLE_CONFIG, reset=False, http_client=mock_http)
        sync.bc = mock_bc
        sync.pg = mock_pg

        yield sync, mock_http, mock_bc, mock_pg, mock_conn


class TestOoDaoRefreshListNewStandardProposal:

    @pytest.mark.asyncio
    async def test_new_standard_proposal_active(self, mocked_oodao, mock_gcs_client):
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_oodao

        now = int(time.time())
        prop_row = make_proposal_row(startts=now - 3600, endts=now + 3600)

        call_count = [0]
        async def multi_fetch(qry=None, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # read_proposals
                return [prop_row]
            elif call_count[0] == 2:
                # read_proposal_deletions
                return []
            elif call_count[0] == 3:
                # read_proposal_checks
                return []
            elif call_count[0] == 4:
                # read_votes_from_db (read_proposal_type_range uses fetchrow)
                return [make_vote_row("0xVoter1", 1, 500)]
            elif call_count[0] == 5:
                # get_delegate_metadata
                return []
            return []

        async def multi_fetchrow(qry=None, *args, **kwargs):
            if "proposal_type_range" in str(call_count[0]) or "min_quorum" in str(qry):
                return {"min_quorum_pct": "1000", "max_quorum_pct": "5000",
                        "min_approval_threshold_pct": "5000", "max_approval_threshold_pct": "7500"}
            if "read_proposal_type" in str(qry) or "ref_uid" in str(qry):
                return None
            if "votable_supply" in str(qry) or "get_votable_supply" in str(qry):
                return {"votable_supply": "100000"}
            return None

        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)
        mock_conn.fetchrow = AsyncMock(side_effect=multi_fetchrow)

        # Validation API returns success
        val_resp = MagicMock()
        val_resp.json.return_value = {"success": True}
        val_resp.raise_for_status = MagicMock()
        mock_http.post = AsyncMock(return_value=val_resp)
        # nonivotes returns 404
        nonivote_resp = MagicMock()
        nonivote_resp.status_code = 404
        mock_http.get = AsyncMock(return_value=nonivote_resp)

        # GCS: proposal doesn't exist yet
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])
        mock_gcs_client.read_ndjson = AsyncMock(return_value=None)

        result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 1
        assert result["skipped"] == 0
        mock_gcs_client.upload_dict.assert_called()

        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "ACTIVE"


class TestOoDaoRefreshListDeletedProposal:

    @pytest.mark.asyncio
    async def test_deleted_proposal_cancelled(self, mocked_oodao, mock_gcs_client):
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_oodao

        now = int(time.time())
        prop_row = make_proposal_row(startts=now - 7200, endts=now - 3600)

        deletion_row = {
            "ref_uid": prop_row["uid"],
            "transaction_hash": "0xdeltx",
            "dao_id": "0xOoDaoAddr",
            "uid": "0xDelUid",
            "deleter": "0xDeleter",
            "chain_id": 1,
            "attestation_time": now - 1800,
        }

        call_count = [0]
        async def multi_fetch(qry=None, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return [prop_row]
            elif call_count[0] == 2:
                return [deletion_row]
            elif call_count[0] == 3:
                return []
            elif call_count[0] == 4:
                return []  # read_votes_from_db
            elif call_count[0] == 5:
                return []  # get_delegate_metadata
            return []

        async def multi_fetchrow(qry=None, *args, **kwargs):
            if "min_quorum" in str(qry):
                return {"min_quorum_pct": "1000", "max_quorum_pct": "5000",
                        "min_approval_threshold_pct": "5000", "max_approval_threshold_pct": "7500"}
            if "ref_uid" in str(qry):
                return None
            if "get_votable_supply" in str(qry):
                return {"votable_supply": "100000"}
            return None

        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)
        mock_conn.fetchrow = AsyncMock(side_effect=multi_fetchrow)

        val_resp = MagicMock()
        val_resp.json.return_value = {"success": True}
        val_resp.raise_for_status = MagicMock()
        mock_http.post = AsyncMock(return_value=val_resp)
        nonivote_resp = MagicMock()
        nonivote_resp.status_code = 404
        mock_http.get = AsyncMock(return_value=nonivote_resp)

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])
        mock_gcs_client.read_ndjson = AsyncMock(return_value=None)

        result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "CANCELLED"


class TestOoDaoRefreshListArchivedSkip:

    @pytest.mark.asyncio
    async def test_archived_proposal_skipped(self, mocked_oodao, mock_gcs_client):
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_oodao

        now = int(time.time())
        prop_row = make_proposal_row(startts=now - 7200, endts=now - 3600)

        call_count = [0]
        async def multi_fetch(qry=None, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return [prop_row]
            elif call_count[0] == 2:
                return []  # deletions
            elif call_count[0] == 3:
                return []  # checks
            return []

        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

        # GCS: proposal exists and is archived
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.metadata = {"liveness": "archived", "hash": "existinghash", "num_of_votes": "5"}
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)

        result = await sync.refresh_list(mock_gcs_client)

        assert result["skipped"] == 1
        assert result["refreshed"] == 0


class TestOoDaoRefreshListCorruptedProposal:

    @pytest.mark.asyncio
    async def test_corrupted_proposal_marked_unqualified(self, mocked_oodao, mock_gcs_client):
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_oodao

        now = int(time.time())
        # Missing title and description
        prop_row = make_proposal_row()
        prop_row["title"] = ""
        prop_row["description"] = ""

        call_count = [0]
        async def multi_fetch(qry=None, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return [prop_row]
            elif call_count[0] == 2:
                return []
            elif call_count[0] == 3:
                return []
            return []

        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs_client)

        assert result["skipped"] == 1
        # Should have uploaded as unqualified
        upload_call = mock_gcs_client.upload_dict.call_args_list[0]
        metadata = upload_call[1]["metadata"]
        assert metadata["liveness"] == "unqualified"


class TestOoDaoRefreshListValidationFailed:

    @pytest.mark.asyncio
    async def test_validation_failed_marks_unqualified(self, mocked_oodao, mock_gcs_client):
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_oodao

        now = int(time.time())
        prop_row = make_proposal_row(startts=now - 3600, endts=now + 3600)

        call_count = [0]
        async def multi_fetch(qry=None, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return [prop_row]
            elif call_count[0] == 2:
                return []
            elif call_count[0] == 3:
                return []  # no checks
            return []

        async def multi_fetchrow(qry=None, *args, **kwargs):
            if "min_quorum" in str(qry):
                return {"min_quorum_pct": "1000", "max_quorum_pct": "5000",
                        "min_approval_threshold_pct": "5000", "max_approval_threshold_pct": "7500"}
            return None

        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)
        mock_conn.fetchrow = AsyncMock(side_effect=multi_fetchrow)

        # Validation API returns failure
        val_resp = MagicMock()
        val_resp.json.return_value = {"success": False}
        val_resp.raise_for_status = MagicMock()
        mock_http.post = AsyncMock(return_value=val_resp)

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs_client)

        assert result["skipped"] == 1
        upload_call = mock_gcs_client.upload_dict.call_args_list[0]
        metadata = upload_call[1]["metadata"]
        assert metadata["liveness"] == "unqualified"


class TestOoDaoRefreshListPendingProposal:

    @pytest.mark.asyncio
    async def test_pending_proposal(self, mocked_oodao, mock_gcs_client):
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_oodao

        now = int(time.time())
        # Both start and end in the future
        prop_row = make_proposal_row(startts=now + 1800, endts=now + 7200)

        call_count = [0]
        async def multi_fetch(qry=None, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return [prop_row]
            elif call_count[0] == 2:
                return []
            elif call_count[0] == 3:
                return [{"proposal_id": prop_row["proposal_id"], "check_uid": "0xCheck", "attestation_time": now}]
            elif call_count[0] == 4:
                return []  # votes
            elif call_count[0] == 5:
                return []  # delegate metadata
            return []

        async def multi_fetchrow(qry=None, *args, **kwargs):
            if "min_quorum" in str(qry):
                return {"min_quorum_pct": "1000", "max_quorum_pct": "5000",
                        "min_approval_threshold_pct": "5000", "max_approval_threshold_pct": "7500"}
            if "ref_uid" in str(qry):
                return None
            return None

        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)
        mock_conn.fetchrow = AsyncMock(side_effect=multi_fetchrow)

        nonivote_resp = MagicMock()
        nonivote_resp.status_code = 404
        mock_http.get = AsyncMock(return_value=nonivote_resp)

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])
        mock_gcs_client.read_ndjson = AsyncMock(return_value=None)

        # last_block_before_timestamp returns -1 for future blocks
        mock_bc.last_block_before_timestamp = AsyncMock(return_value=-1)

        result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "PENDING"


def _setup_ended_proposal(mock_conn, mock_http, mock_bc, mock_gcs_client,
                          voting_module="standard", votes=None,
                          approved_prop_type=None):
    """Helper to set up an ended proposal with given voting module and votes."""
    now = int(time.time())
    prop_row = make_proposal_row(
        startts=now - 7200, endts=now - 3600,
        kwargs=json.dumps({"voting_module": voting_module}),
    )

    if votes is None:
        votes = []

    call_count = [0]
    async def multi_fetch(qry=None, *args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            return [prop_row]
        elif call_count[0] == 2:
            return []  # deletions
        elif call_count[0] == 3:
            return [{"proposal_id": prop_row["proposal_id"], "check_uid": "0xC", "attestation_time": now}]
        elif call_count[0] == 4:
            return votes  # read_votes_from_db
        elif call_count[0] == 5:
            return []  # get_delegate_metadata
        elif call_count[0] == 6:
            # get_vp_snapshot_all_delegates_from_db
            return [{"addr": v["voter"].lower(), "vp": str(v["weight"])} for v in votes]
        return []

    fetchrow_count = [0]
    async def multi_fetchrow(qry=None, *args, **kwargs):
        fetchrow_count[0] += 1
        if "min_quorum" in str(qry):
            return {"min_quorum_pct": "1000", "max_quorum_pct": "5000",
                    "min_approval_threshold_pct": "5000", "max_approval_threshold_pct": "7500"}
        if "ref_uid" in str(qry):
            if approved_prop_type and fetchrow_count[0] <= 3:
                return {"data": "0xPropType", "decoded_attestation": json.dumps(approved_prop_type)}
            return None
        if "get_votable_supply" in str(qry):
            return {"votable_supply": "100000"}
        return None

    mock_conn.fetch = AsyncMock(side_effect=multi_fetch)
    mock_conn.fetchrow = AsyncMock(side_effect=multi_fetchrow)

    nonivote_resp = MagicMock()
    nonivote_resp.status_code = 404
    mock_http.get = AsyncMock(return_value=nonivote_resp)

    mock_blob = MagicMock()
    mock_blob.exists.return_value = False
    mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
    mock_gcs_client.list_blobs = AsyncMock(return_value=[])
    mock_gcs_client.read_ndjson = AsyncMock(return_value=None)

    return prop_row


class TestOoDaoOptimisticLifecycle:

    @pytest.mark.asyncio
    async def test_optimistic_quorum_not_met_succeeds(self, mocked_oodao, mock_gcs_client):
        """Optimistic proposals pass by default if quorum is not met."""
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_oodao

        prop_type = {"eas_uid": "0xPT", "name": "Opt", "class": "OPTIMISTIC",
                     "quorum": 5000, "approval_threshold": 5000, "threshold": 3000}
        _setup_ended_proposal(mock_conn, mock_http, mock_bc, mock_gcs_client,
                              voting_module="optimistic",
                              votes=[make_vote_row("0xV1", 1, 100)],
                              approved_prop_type=prop_type)

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "SUCCEEDED"
        assert uploaded["quorum_check"] is False

    @pytest.mark.asyncio
    async def test_optimistic_quorum_met_against_exceeds_threshold_defeated(self, mocked_oodao, mock_gcs_client):
        """Optimistic proposal defeated when quorum met and against > threshold."""
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_oodao

        # quorum=100/10000 * 100000 = 1000, threshold=100/10000 * 100000 = 1000
        prop_type = {"eas_uid": "0xPT", "name": "Opt", "class": "OPTIMISTIC",
                     "quorum": 100, "approval_threshold": 100, "threshold": 100}
        _setup_ended_proposal(mock_conn, mock_http, mock_bc, mock_gcs_client,
                              voting_module="optimistic",
                              votes=[
                                  make_vote_row("0xV1", 0, 50000),  # against
                                  make_vote_row("0xV2", 1, 60000),  # for
                              ],
                              approved_prop_type=prop_type)

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "DEFEATED"
        assert uploaded["quorum_check"] is True

    @pytest.mark.asyncio
    async def test_optimistic_quorum_met_against_below_threshold_succeeds(self, mocked_oodao, mock_gcs_client):
        """Optimistic proposal succeeds when quorum met but against <= threshold."""
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_oodao

        prop_type = {"eas_uid": "0xPT", "name": "Opt", "class": "OPTIMISTIC",
                     "quorum": 100, "approval_threshold": 100, "threshold": 5000}
        _setup_ended_proposal(mock_conn, mock_http, mock_bc, mock_gcs_client,
                              voting_module="optimistic",
                              votes=[
                                  make_vote_row("0xV1", 0, 100),    # against (small)
                                  make_vote_row("0xV2", 1, 60000),  # for
                              ],
                              approved_prop_type=prop_type)

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "SUCCEEDED"


class TestOoDaoStandardEndedLifecycle:

    @pytest.mark.asyncio
    async def test_standard_ended_passed(self, mocked_oodao, mock_gcs_client):
        """Standard proposal passes when quorum and approval threshold met."""
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_oodao

        # quorum=100/10000*100000=1000, approval=100/10000*100000=1000
        prop_type = {"eas_uid": "0xPT", "name": "Std", "class": "STANDARD",
                     "quorum": 100, "approval_threshold": 100}
        _setup_ended_proposal(mock_conn, mock_http, mock_bc, mock_gcs_client,
                              voting_module="standard",
                              votes=[
                                  make_vote_row("0xV1", 1, 50000),  # for
                                  make_vote_row("0xV2", 0, 10000),  # against
                              ],
                              approved_prop_type=prop_type)

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "PASSED"
        assert uploaded["quorum_check"] is True
        assert uploaded["approval_check"] is True

    @pytest.mark.asyncio
    async def test_standard_ended_defeated(self, mocked_oodao, mock_gcs_client):
        """Standard proposal defeated when approval threshold not met."""
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_oodao

        # quorum=100/10000*100000=1000, approval=5000/10000*100000=50000
        prop_type = {"eas_uid": "0xPT", "name": "Std", "class": "STANDARD",
                     "quorum": 100, "approval_threshold": 5000}
        _setup_ended_proposal(mock_conn, mock_http, mock_bc, mock_gcs_client,
                              voting_module="standard",
                              votes=[
                                  make_vote_row("0xV1", 1, 100),    # for (tiny)
                                  make_vote_row("0xV2", 0, 50000),  # against
                              ],
                              approved_prop_type=prop_type)

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "DEFEATED"


class TestOoDaoApprovalVotingFlow:

    @pytest.mark.asyncio
    async def test_approval_voting_with_comma_support(self, mocked_oodao, mock_gcs_client):
        """Test approval voting flow with comma-separated support values."""
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_oodao

        prop_type = {"eas_uid": "0xPT", "name": "Appr", "class": "APPROVAL",
                     "quorum": 100, "approval_threshold": 100}
        votes = [
            make_vote_row("0xV1", "1,2", 5000),
            make_vote_row("0xV2", "0", 3000),
        ]
        _setup_ended_proposal(mock_conn, mock_http, mock_bc, mock_gcs_client,
                              voting_module="approval",
                              votes=votes,
                              approved_prop_type=prop_type)

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert "outcome" in uploaded
        # APPROVAL lifecycle: quorum met → SUCCEEDED (no THRESHOLD criteria)
        assert uploaded["lifecycle_stage"] == "SUCCEEDED"

    @pytest.mark.asyncio
    async def test_approval_quorum_not_met_defeated(self, mocked_oodao, mock_gcs_client):
        """Approval proposal defeated when quorum not met."""
        sync, mock_http, mock_bc, mock_pg, mock_conn = mocked_oodao

        # quorum=5000/10000*100000=50000 — hard to meet
        prop_type = {"eas_uid": "0xPT", "name": "Appr", "class": "APPROVAL",
                     "quorum": 5000, "approval_threshold": 100}
        votes = [make_vote_row("0xV1", "1", 100)]
        _setup_ended_proposal(mock_conn, mock_http, mock_bc, mock_gcs_client,
                              voting_module="approval",
                              votes=votes,
                              approved_prop_type=prop_type)

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["lifecycle_stage"] == "DEFEATED"
        assert uploaded["quorum_check"] is False
