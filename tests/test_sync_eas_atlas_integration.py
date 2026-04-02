"""
Integration tests for EASAtlasSync.refresh_list() with fully mocked externals.
"""

import pytest
import time
import json
from unittest.mock import AsyncMock, MagicMock, patch
from collections import defaultdict


SAMPLE_CONFIG = {
    "schema": "optimism",
    "dao_slug": "Optimism",
    "index_tenant_prefix": "op",
    "features": {"oodao": False},
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


def make_attestation(proposal_id="eas-prop-1", proposal_type="STANDARD"):
    now = int(time.time())
    return {
        "chain_id": 10,
        "attestation": {
            "uid": "0x1234567890abcdef" + "0" * 48,
            "id": proposal_id,
            "data": "0x...",
            "proposer": "0xProposerAddr",
            "start_block": 140000000,
            "time": now - 7200,
        },
        "decoded_data": {
            "description": "# EAS Proposal\n\nBody text here",
            "proposal_type": proposal_type,
            "start_block": 140000000,
        },
        "schema": {
            "resolver": "0xResolverAddr",
        },
    }


@pytest.fixture
def mocked_eas_atlas():
    with patch("cpls.sync.PostgreSQLClient") as MockPG, \
         patch("cpls.sync.BlockCacheClient") as MockBC, \
         patch("cpls.sync.load_tenant_config", return_value=SAMPLE_CONFIG):

        mock_http = AsyncMock()
        mock_bc = MagicMock()
        mock_bc.clear_lru = MagicMock()
        mock_bc.get_blocktime = AsyncMock(return_value=int(time.time()) - 3600)
        mock_bc.get_ens = AsyncMock(return_value={"name": "proposer.eth"})
        mock_bc.get_decoded_eas = AsyncMock(return_value=make_attestation())
        MockBC.return_value = mock_bc

        mock_pg = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.fetchrow = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        mock_pool.acquire.return_value = FakeAcquire(mock_conn)
        mock_pg.connect = AsyncMock(return_value=mock_pool)
        MockPG.return_value = mock_pg

        from cpls.sync_eas_atlas import EASAtlasSync
        sync = EASAtlasSync("optimism", SAMPLE_CONFIG, reset=False, http_client=mock_http)
        sync.bc = mock_bc
        sync.pg = mock_pg

        yield sync, mock_bc, mock_pg, mock_conn


class TestEASAtlasRefreshListNew:

    @pytest.mark.asyncio
    async def test_new_standard_proposal(self, mocked_eas_atlas, mock_gcs_client):
        sync, mock_bc, mock_pg, mock_conn = mocked_eas_atlas

        # DB returns: one create attestation, a govless proposal set, citizens, and votes
        call_count = [0]
        async def multi_fetch(*args, **kwargs):
            call_count[0] += 1
            # Call order: read_govless_proposal_set, read_citizens(s8), read_citizens(s9),
            #             read_proposal_create_attestations, read_votes
            if call_count[0] == 1:
                # read_govless_proposal_set
                return [{"govless_proposal_id": "eas-prop-1"}]
            elif call_count[0] == 2:
                # read_citizens s8
                return [
                    {"addr": "0xCitizen1", "vp": 1, "citizen_type": "badgeholder", "name": "Cit1", "image": None},
                    {"addr": "0xCitizen2", "vp": 1, "citizen_type": "badgeholder", "name": "Cit2", "image": None},
                ]
            elif call_count[0] == 3:
                # read_citizens s9
                return [
                    {"addr": "0xCitizen1", "vp": 1, "citizen_type": "badgeholder", "name": "Cit1", "image": None},
                ]
            elif call_count[0] == 4:
                # read_proposal_create_attestations
                return [{"created_attestation_hash": "0xAttestation1"}]
            elif call_count[0] == 5:
                # read_votes for the proposal
                return [
                    {"voter": "0xCitizen1", "support": "1", "weight": "1", "reason": "", "params": None, "citizen_type": "badgeholder", "name": "Cit1", "image": None},
                ]
            return []

        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

        # GCS: proposal doesn't exist yet
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])

        # Attestation from BlockCache
        mock_bc.get_decoded_eas = AsyncMock(return_value=make_attestation("eas-prop-1", "STANDARD"))

        # Blocktimes: proposal ended in the past
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 1
        assert result["skipped"] == 0

        # Check proposal was uploaded with outcome
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert "outcome" in uploaded
        assert uploaded["title"] == "EAS Proposal"
        assert uploaded["proposer_ens"] == {"name": "proposer.eth"}

        # Check hasnt_voted was uploaded (Citizen2 didn't vote)
        ndjson_calls = mock_gcs_client.upload_ndjson.call_args_list
        hasnt_voted_calls = [c for c in ndjson_calls if "hasnt_voted" in c[0][1]]
        assert len(hasnt_voted_calls) >= 1


class TestEASAtlasRefreshListSkipArchived:

    @pytest.mark.asyncio
    async def test_archived_proposal_skipped(self, mocked_eas_atlas, mock_gcs_client):
        sync, mock_bc, mock_pg, mock_conn = mocked_eas_atlas

        call_count = [0]
        async def multi_fetch(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return [{"govless_proposal_id": "eas-prop-1"}]
            elif call_count[0] in (2, 3):
                return []
            elif call_count[0] == 4:
                return [{"created_attestation_hash": "0xAttestation1"}]
            return []

        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

        mock_bc.get_decoded_eas = AsyncMock(return_value=make_attestation("eas-prop-1"))

        # GCS: proposal exists and is archived
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.metadata = {"liveness": "archived", "hash": "existinghash", "num_of_votes": "3"}
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)

        result = await sync.refresh_list(mock_gcs_client)

        assert result["skipped"] == 1
        assert result["refreshed"] == 0
        mock_gcs_client.upload_dict.assert_not_called()


class TestEASAtlasRefreshListNullAttestation:

    @pytest.mark.asyncio
    async def test_null_attestation_skipped(self, mocked_eas_atlas, mock_gcs_client):
        sync, mock_bc, mock_pg, mock_conn = mocked_eas_atlas

        call_count = [0]
        async def multi_fetch(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return []
            elif call_count[0] in (2, 3):
                return []
            elif call_count[0] == 4:
                return [{"created_attestation_hash": "0xBadAttestation"}]
            return []

        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

        # BlockCache returns None for this attestation
        mock_bc.get_decoded_eas = AsyncMock(return_value=None)

        result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 0
        assert result["skipped"] == 0  # Null attestations are `continue`d, not counted as skipped


class TestEASAtlasApprovalVotes:

    @pytest.mark.asyncio
    async def test_approval_vote_processing(self, mocked_eas_atlas, mock_gcs_client):
        sync, mock_bc, mock_pg, mock_conn = mocked_eas_atlas

        call_count = [0]
        async def multi_fetch(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return [{"govless_proposal_id": "eas-approval-1"}]
            elif call_count[0] in (2, 3):
                return []
            elif call_count[0] == 4:
                return [{"created_attestation_hash": "0xApprovalAtt"}]
            elif call_count[0] == 5:
                # Votes with approval-style support (JSON array of chosen options)
                return [
                    {"voter": "0xV1", "support": json.dumps([0, 2]), "weight": "1", "reason": "", "params": None, "citizen_type": "badgeholder", "name": "V1", "image": None},
                ]
            return []

        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

        mock_bc.get_decoded_eas = AsyncMock(return_value=make_attestation("eas-approval-1", "APPROVAL"))
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs_client)

        assert result["refreshed"] == 1
        uploaded = mock_gcs_client.upload_dict.call_args_list[0][0][0]
        assert uploaded["outcome"] is not None
