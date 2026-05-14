"""
Deep coverage tests for EASAtlasSync:
- Zero-uid attestation skip (lines 85-86)
- reuse_tally path (lines 119-124)
- Unknown proposal type raises (line 162)
- Hash unchanged skip (lines 170-173)
- Chain ID citizens branches: chain_id != 10 (line 182), s8 block range (line 180)
"""

import pytest
import time
import json
from unittest.mock import AsyncMock, MagicMock, patch


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


def make_attestation(proposal_id="p1", proposal_type="STANDARD", uid=None):
    if uid is None:
        uid = "0x1234567890abcdef" + "0" * 48
    return {
        "chain_id": 10,
        "attestation": {
            "uid": uid,
            "id": proposal_id,
            "data": "0x...",
            "proposer": "0xProposerAddr",
            "start_block": 140000000,
            "time": int(time.time()) - 7200,
        },
        "decoded_data": {
            "description": "# Deep Test\n\nBody",
            "proposal_type": proposal_type,
            "start_block": 140000000,
        },
        "schema": {"resolver": "0xResolverAddr"},
    }


@pytest.fixture
def atlas_sync():
    with patch("cpls.sync.PostgreSQLClient") as MockPG, \
         patch("cpls.sync.BlockCacheClient") as MockBC, \
         patch("cpls.sync.load_tenant_config", return_value=SAMPLE_CONFIG):

        mock_http = AsyncMock()
        mock_bc = MagicMock()
        mock_bc.clear_lru = MagicMock()
        mock_bc.get_blocktime = AsyncMock(return_value=int(time.time()) - 3600)
        mock_bc.get_ens = AsyncMock(return_value=None)
        MockBC.return_value = mock_bc

        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_pool = MagicMock()
        mock_pool.acquire.return_value = FakeAcquire(mock_conn)
        mock_pg = MagicMock()
        mock_pg.connect = AsyncMock(return_value=mock_pool)
        MockPG.return_value = mock_pg

        from cpls.sync_eas_atlas import EASAtlasSync
        sync = EASAtlasSync("optimism", SAMPLE_CONFIG, reset=False, http_client=mock_http)
        sync.bc = mock_bc
        sync.pg = mock_pg

        yield sync, mock_bc, mock_conn


@pytest.fixture
def mock_gcs_client():
    gcs = MagicMock()
    gcs.upload_dict = AsyncMock(return_value=True)
    gcs.upload_ndjson = AsyncMock(return_value=True)
    gcs.read_ndjson = AsyncMock(return_value=None)
    gcs.read_dict = AsyncMock(return_value=None)
    gcs.list_blobs = AsyncMock(return_value=[])
    return gcs


class TestZeroUidSkip:

    @pytest.mark.asyncio
    async def test_zero_uid_attestation_is_skipped(self, atlas_sync, mock_gcs_client):
        """Lines 85-86: attestation with all-zero uid is skipped."""
        sync, mock_bc, mock_conn = atlas_sync

        zero_uid = "0x" + "0" * 64
        mock_bc.get_decoded_eas = AsyncMock(return_value=make_attestation("p-zero", uid=zero_uid))

        call_count = [0]
        async def multi_fetch(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return []  # govless set
            if call_count[0] in (2, 3):
                return []  # citizens
            if call_count[0] == 4:
                return [{"created_attestation_hash": "0xAtt1"}]
            return []
        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 0
        mock_gcs_client.upload_dict.assert_not_called()


class TestReuseTallyPath:

    @pytest.mark.asyncio
    async def test_reuse_tally_reads_existing_outcome(self, atlas_sync, mock_gcs_client):
        """Lines 119-124: when vote count matches, reuse existing outcome from GCS."""
        sync, mock_bc, mock_conn = atlas_sync

        att = make_attestation("p-reuse", "STANDARD")
        mock_bc.get_decoded_eas = AsyncMock(side_effect=lambda c, u: att if c == 10 else None)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        call_count = [0]
        async def multi_fetch(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return []  # govless set
            if call_count[0] in (2, 3):
                return []  # citizens
            if call_count[0] == 4:
                return [{"created_attestation_hash": "0xAtt2"}]
            if call_count[0] == 5:
                return [
                    {"voter": "0xV1", "support": "1", "weight": "1", "reason": "",
                     "params": None, "citizen_type": "badgeholder", "name": "V1", "image": None},
                ]
            return []
        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

        # Blob exists with num_of_votes=1 (matching what read_votes returns)
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.name = "data/optimism/proposal/eas-atlas/raw/p-reuse.json.gz"
        mock_blob.metadata = {"liveness": "live", "hash": "different-hash", "num_of_votes": "1"}
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])

        existing_data = {
            "id": "p-reuse",
            "outcome": {"badgeholder": {1: 1}},
            "data_eng_properties": {"liveness": "live", "hash": "different-hash", "num_of_votes": 1}
        }
        mock_gcs_client.read_dict = AsyncMock(return_value=existing_data)

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        # votes overwrite should NOT have been called (reuse_tally skips it)
        votes_calls = [c for c in mock_gcs_client.upload_ndjson.call_args_list if "votes" in c[0][1]]
        assert len(votes_calls) == 0


class TestUnknownProposalTypeRaises:

    @pytest.mark.asyncio
    async def test_unknown_type_raises_exception(self, atlas_sync, mock_gcs_client):
        """Line 162: UNKNOWN proposal type raises an exception."""
        sync, mock_bc, mock_conn = atlas_sync

        mock_bc.get_decoded_eas = AsyncMock(return_value=make_attestation("p-bad", "UNKNOWN_TYPE"))
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        call_count = [0]
        async def multi_fetch(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return []
            if call_count[0] in (2, 3):
                return []
            if call_count[0] == 4:
                return [{"created_attestation_hash": "0xAtt3"}]
            if call_count[0] == 5:
                return [
                    {"voter": "0xV1", "support": "1", "weight": "1", "reason": "",
                     "params": None, "citizen_type": "badgeholder", "name": "V1", "image": None},
                ]
            return []
        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])

        with pytest.raises(Exception, match="Unknown EAS-Atlas proposal type"):
            await sync.refresh_list(mock_gcs_client)


class TestHashUnchangedSkip:

    @pytest.mark.asyncio
    async def test_hash_unchanged_increments_skipped(self, atlas_sync, mock_gcs_client):
        """Lines 170-173: when hash matches and reset=False, proposal is skipped."""
        sync, mock_bc, mock_conn = atlas_sync
        sync.reset = False

        att = make_attestation("p-same", "STANDARD")
        mock_bc.get_decoded_eas = AsyncMock(side_effect=lambda c, u: att if c == 10 else None)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        call_count = [0]
        async def multi_fetch(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return []
            if call_count[0] in (2, 3):
                return []
            if call_count[0] == 4:
                return [{"created_attestation_hash": "0xAtt4"}]
            if call_count[0] == 5:
                return []  # no votes
            return []
        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

        # We need to compute the real hash of what the proposal will look like
        # so we can plant it. Instead, patch check_existing_proposal_hash to raise SkipProposal.
        from cpls.sync import SkipProposal
        with patch.object(sync, 'check_existing_proposal_hash',
                          side_effect=SkipProposal("content state is unchanged", proposal_id="p-same")):
            mock_blob = MagicMock()
            mock_blob.exists.return_value = False
            mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
            mock_gcs_client.list_blobs = AsyncMock(return_value=[])

            result = await sync.refresh_list(mock_gcs_client)
            assert result["skipped"] == 1
            assert result["refreshed"] == 0


class TestReuseTallyNoneData:

    @pytest.mark.asyncio
    async def test_reuse_tally_none_read_dict_raises(self, atlas_sync, mock_gcs_client):
        """Line 122: when reuse_tally and read_dict returns None, raise Exception."""
        sync, mock_bc, mock_conn = atlas_sync

        mock_bc.get_decoded_eas = AsyncMock(return_value=make_attestation("p-null", "STANDARD"))
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        call_count = [0]
        async def multi_fetch(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return []
            if call_count[0] in (2, 3):
                return []
            if call_count[0] == 4:
                return [{"created_attestation_hash": "0xAtt"}]
            if call_count[0] == 5:
                return [
                    {"voter": "0xV1", "support": "1", "weight": "1", "reason": "",
                     "params": None, "citizen_type": "badgeholder", "name": "V1", "image": None},
                ]
            return []
        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.name = "data/optimism/proposal/eas-atlas/raw/p-null.json.gz"
        mock_blob.metadata = {"liveness": "live", "hash": "different-hash", "num_of_votes": "1"}
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])

        # read_dict returns None → should raise
        mock_gcs_client.read_dict = AsyncMock(return_value=None)

        with pytest.raises(Exception, match="None for existing_proposal_data"):
            await sync.refresh_list(mock_gcs_client)


class TestCitizensS9BlockRange:

    @pytest.mark.asyncio
    async def test_chain10_s9_block_range_uses_citizens_s9(self, atlas_sync, mock_gcs_client):
        """Line 178: chain_id==10 AND block_number >= S9_BLOCK_NUMBER uses citizens_s9."""
        from cpls.sync_eas_atlas import S9_BLOCK_NUMBER
        sync, mock_bc, mock_conn = atlas_sync

        att = make_attestation("p-s9")
        att["attestation"]["start_block"] = S9_BLOCK_NUMBER  # at or above S9 threshold
        att["decoded_data"]["start_block"] = S9_BLOCK_NUMBER
        mock_bc.get_decoded_eas = AsyncMock(side_effect=lambda c, u: att if c == 10 else None)
        past = int(time.time()) - 7200
        mock_bc.get_blocktime = AsyncMock(return_value=past)

        call_count = [0]
        async def multi_fetch(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return []  # govless set
            if call_count[0] == 2:
                return []  # s8 citizens
            if call_count[0] == 3:
                return [{"addr": "0xS9Citizen", "vp": 1, "citizen_type": "badgeholder",
                         "name": "S9", "image": None}]  # s9 citizens
            if call_count[0] == 4:
                return [{"created_attestation_hash": "0xS9Att"}]
            if call_count[0] == 5:
                return []  # votes
            return []
        mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
        mock_gcs_client.list_blobs = AsyncMock(return_value=[])

        result = await sync.refresh_list(mock_gcs_client)
        assert result["refreshed"] == 1
        # S9Citizen should be in hasnt_voted
        hasnt_voted_calls = [c for c in mock_gcs_client.upload_ndjson.call_args_list
                             if "hasnt_voted" in c[0][1]]
        assert len(hasnt_voted_calls) == 1
        hasnt_voted_data = hasnt_voted_calls[0][0][0]
        assert any(r["addr"] == "0xS9Citizen" for r in hasnt_voted_data)


class TestCitizensBranchNonOp10:

    @pytest.mark.asyncio
    async def test_non_op10_chain_uses_s8_citizens(self, mock_gcs_client):
        """Line 182: chain_id != 10 uses citizens_s8 fallback."""
        config = {**SAMPLE_CONFIG, "deployment": {
            "chain_id": 1,
            "gov": {"address": "0xGov"},
            "token": {"address": "0xToken"},
        }}
        with patch("cpls.sync.PostgreSQLClient") as MockPG, \
             patch("cpls.sync.BlockCacheClient") as MockBC, \
             patch("cpls.sync.load_tenant_config", return_value=config):

            mock_bc = MagicMock()
            mock_bc.clear_lru = MagicMock()
            mock_bc.get_blocktime = AsyncMock(return_value=int(time.time()) - 3600)
            mock_bc.get_ens = AsyncMock(return_value=None)
            MockBC.return_value = mock_bc

            mock_conn = MagicMock()
            mock_pool = MagicMock()
            mock_pool.acquire.return_value = FakeAcquire(mock_conn)
            mock_pg = MagicMock()
            mock_pg.connect = AsyncMock(return_value=mock_pool)
            MockPG.return_value = mock_pg

            from cpls.sync_eas_atlas import EASAtlasSync
            sync = EASAtlasSync("optimism", config, reset=False, http_client=AsyncMock())
            sync.bc = mock_bc
            sync.pg = mock_pg

            att = make_attestation("p-chain1", "STANDARD")
            att["chain_id"] = 1
            mock_bc.get_decoded_eas = AsyncMock(side_effect=lambda c, u: att if c == 1 else None)
            past = int(time.time()) - 7200
            mock_bc.get_blocktime = AsyncMock(return_value=past)

            call_count = [0]
            async def multi_fetch(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] == 1:
                    return []
                if call_count[0] == 2:
                    return [{"addr": "0xCitizen", "vp": 1, "citizen_type": "badgeholder", "name": "C", "image": None}]
                if call_count[0] == 3:
                    return []
                if call_count[0] == 4:
                    return [{"created_attestation_hash": "0xAtt5"}]
                if call_count[0] == 5:
                    return []
                return []
            mock_conn.fetch = AsyncMock(side_effect=multi_fetch)

            mock_blob = MagicMock()
            mock_blob.exists.return_value = False
            mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)
            mock_gcs_client.list_blobs = AsyncMock(return_value=[])

            result = await sync.refresh_list(mock_gcs_client)
            assert result["refreshed"] == 1
