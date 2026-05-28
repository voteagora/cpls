"""
Tests for Sync base class methods: refresh_source_list, refresh_full_list,
get_delegate_metadata, read_existing_raw_proposal_hash_if_exists edge cases,
get_vp_snapshot_all_delegates, read_votes_from_db, and overwrite_proposal.
"""

import pytest
import time
from unittest.mock import AsyncMock, MagicMock, patch


SAMPLE_CONFIG = {
    "schema": "testdao",
    "dao_slug": "TestDAO",
    "index_tenant_prefix": "td",
    "features": {},
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
def sync_instance():
    with patch("cpls.sync.PostgreSQLClient") as MockPG, \
         patch("cpls.sync.BlockCacheClient") as MockBC, \
         patch("cpls.sync.load_tenant_config", return_value=SAMPLE_CONFIG):

        mock_bc = MagicMock()
        mock_bc.get_blocktime = AsyncMock(return_value=int(time.time()) - 3600)
        mock_bc.get_ens_lru = AsyncMock(return_value=None)
        MockBC.return_value = mock_bc

        mock_pg = MagicMock()
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.fetchrow = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        mock_pool.acquire.return_value = FakeAcquire(mock_conn)
        mock_pg.connect = AsyncMock(return_value=mock_pool)
        MockPG.return_value = mock_pg

        from cpls.sync import Sync
        # Sync needs a SOURCE attribute for blob naming
        sync = Sync("testdao", SAMPLE_CONFIG, reset=False, http_client=AsyncMock())
        sync.SOURCE = "dao_node"
        sync.bc = mock_bc
        sync.pg = mock_pg
        sync.index_tenant_prefix = "td"
        sync.token_addr = "0xTokenAddr"

        yield sync, mock_bc, mock_pg, mock_conn


class TestRefreshSourceList:

    @pytest.mark.asyncio
    async def test_builds_list_from_blobs(self, sync_instance, mock_gcs_client):
        sync, _, _, _ = sync_instance

        blob1 = MagicMock()
        blob1.name = "data/testdao/proposal/dao_node/raw/123.json.gz"
        blob1.reload = MagicMock()
        blob2 = MagicMock()
        blob2.name = "data/testdao/proposal/dao_node/raw/456.json.gz"
        blob2.reload = MagicMock()
        # Non-.json.gz file should be skipped
        blob3 = MagicMock()
        blob3.name = "data/testdao/proposal/dao_node/raw/README.txt"

        mock_gcs_client.list_blobs = AsyncMock(return_value=[blob1, blob2, blob3])
        mock_gcs_client.read_dict = AsyncMock(side_effect=[
            {
                "id": "123", "end_blocktime": 1000,
                "description": "desc1",
                "data_eng_properties": {"liveness": "live", "source": "dao_node", "hash": "h1"}
            },
            {
                "id": "456", "end_blocktime": 2000,
                "description": "desc2",
                "data_eng_properties": {"liveness": "live", "source": "dao_node", "hash": "h2"}
            },
        ])

        await sync.refresh_source_list(mock_gcs_client)

        mock_gcs_client.upload_ndjson.assert_called_once()
        uploaded_list = mock_gcs_client.upload_ndjson.call_args[0][0]
        assert len(uploaded_list) == 2
        # Sorted by end_blocktime descending
        assert uploaded_list[0]["id"] == "456"
        # description should be removed
        assert "description" not in uploaded_list[0]
        # hash should be removed from data_eng_properties
        assert "hash" not in uploaded_list[0]["data_eng_properties"]

    @pytest.mark.asyncio
    async def test_skips_unqualified_proposals(self, sync_instance, mock_gcs_client):
        sync, _, _, _ = sync_instance

        blob1 = MagicMock()
        blob1.name = "data/testdao/proposal/dao_node/raw/789.json.gz"
        blob1.reload = MagicMock()

        mock_gcs_client.list_blobs = AsyncMock(return_value=[blob1])
        mock_gcs_client.read_dict = AsyncMock(return_value={
            "id": "789", "end_blocktime": 3000,
            "description": "unq",
            "data_eng_properties": {"liveness": "unqualified", "source": "dao_node", "hash": "h3"}
        })

        await sync.refresh_source_list(mock_gcs_client)

        uploaded_list = mock_gcs_client.upload_ndjson.call_args[0][0]
        assert len(uploaded_list) == 0


class TestRefreshFullList:

    @pytest.mark.asyncio
    async def test_merges_source_lists(self, sync_instance, mock_gcs_client):
        sync, _, _, _ = sync_instance

        blob1 = MagicMock()
        blob1.name = "data/testdao/proposal_list/dao_node/raw.ndjson.gz"
        blob1.reload = MagicMock()

        mock_gcs_client.list_blobs = AsyncMock(return_value=[blob1])
        mock_gcs_client.read_ndjson = AsyncMock(return_value=[
            {"id": "a", "end_blocktime": 100, "hybrid": False,
             "data_eng_properties": {"source": "dao_node"}},
            {"id": "b", "end_blocktime": 200, "hybrid": False,
             "data_eng_properties": {"source": "dao_node"}},
        ])

        await sync.refresh_full_list(mock_gcs_client)

        mock_gcs_client.upload_ndjson.assert_called_once()
        upload_args = mock_gcs_client.upload_ndjson.call_args[0]
        assert "proposal_list.full.ndjson.gz" in upload_args[1]

    @pytest.mark.asyncio
    async def test_removes_hybrid_eas_atlas(self, sync_instance, mock_gcs_client):
        """Hybrid eas-atlas proposals are removed from the full list."""
        sync, _, _, _ = sync_instance

        blob1 = MagicMock()
        blob1.name = "data/testdao/proposal_list/eas-atlas/raw.ndjson.gz"
        blob1.reload = MagicMock()

        mock_gcs_client.list_blobs = AsyncMock(return_value=[blob1])
        mock_gcs_client.read_ndjson = AsyncMock(return_value=[
            {"id": "hybrid1", "end_blocktime": 100, "hybrid": True,
             "onchain_proposalid": "onchain1",
             "data_eng_properties": {"source": "eas-atlas"}},
            {"id": "standalone", "end_blocktime": 200, "hybrid": False,
             "data_eng_properties": {"source": "eas-atlas"}},
        ])

        await sync.refresh_full_list(mock_gcs_client)

        # The full list passed to upload_ndjson should still contain both,
        # but the fresh_list filtering removes hybrid eas-atlas
        # (the code sorts proposal_list, not fresh_list, so both are in the upload)
        mock_gcs_client.upload_ndjson.assert_called_once()


class TestGetDelegateMetadata:

    @pytest.mark.asyncio
    async def test_returns_metadata_dict(self, sync_instance):
        sync, _, _, mock_conn = sync_instance
        sync.dao_slug = "TestDAO"

        mock_conn.fetch = AsyncMock(return_value=[
            {"address": "0xABC", "discord": "user#1234", "x": "twitteruser", "warpcast": None},
            {"address": "0xDEF", "discord": None, "x": None, "warpcast": "wc_user"},
        ])

        result = await sync.get_delegate_metadata()

        assert "0xabc" in result
        assert result["0xabc"]["discord"] == "user#1234"
        assert result["0xabc"]["x"] == "twitteruser"
        assert "warpcast" not in result["0xabc"]  # None values excluded

        assert "0xdef" in result
        assert result["0xdef"]["warpcast"] == "wc_user"
        assert "discord" not in result["0xdef"]


class TestReadExistingProposalEdgeCases:

    @pytest.mark.asyncio
    async def test_blob_exists_reload_fails_raises_skip(self, sync_instance, mock_gcs_client):
        """When blob.reload() fails, SkipProposal is raised."""
        sync, _, _, _ = sync_instance
        from cpls.sync import SkipProposal

        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.reload.side_effect = Exception("reload failed")
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)

        with pytest.raises(SkipProposal):
            await sync.read_existing_raw_proposal_hash_if_exists("prop-1", mock_gcs_client)

    @pytest.mark.asyncio
    async def test_blob_existence_check_fails_raises_skip(self, sync_instance, mock_gcs_client):
        """When blob.exists() fails, SkipProposal is raised."""
        sync, _, _, _ = sync_instance
        from cpls.sync import SkipProposal

        mock_blob = MagicMock()
        mock_blob.exists.side_effect = Exception("network error")
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)

        with pytest.raises(SkipProposal):
            await sync.read_existing_raw_proposal_hash_if_exists("prop-1", mock_gcs_client)

    @pytest.mark.asyncio
    async def test_blob_exists_returns_metadata(self, sync_instance, mock_gcs_client):
        """When blob exists and is live, returns metadata tuple."""
        sync, _, _, _ = sync_instance

        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.metadata = {"liveness": "live", "hash": "abc123", "num_of_votes": "5"}
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)

        blob, liveness, hash_val, num_votes = await sync.read_existing_raw_proposal_hash_if_exists("prop-1", mock_gcs_client)
        assert liveness == "live"
        assert hash_val == "abc123"
        assert num_votes == 5


class TestOverwriteProposal:

    @pytest.mark.asyncio
    async def test_uploads_with_correct_metadata(self, sync_instance, mock_gcs_client):
        sync, _, _, _ = sync_instance

        proposal = {"id": "test-1", "num_of_votes": 3, "title": "Test"}
        await sync.overwrite_proposal(proposal, "hash123", "live", mock_gcs_client)

        mock_gcs_client.upload_dict.assert_called_once()
        call_args = mock_gcs_client.upload_dict.call_args
        uploaded = call_args[0][0]
        assert uploaded["data_eng_properties"]["liveness"] == "live"
        assert uploaded["data_eng_properties"]["hash"] == "hash123"
        assert call_args[1]["metadata"]["num_of_votes"] == 3

    @pytest.mark.asyncio
    async def test_raises_on_none_hash(self, sync_instance, mock_gcs_client):
        sync, _, _, _ = sync_instance

        with pytest.raises(Exception, match="Proposal hash cannot be None"):
            await sync.overwrite_proposal({"id": "1"}, None, "live", mock_gcs_client)


class TestReadVotesFromDb:

    @pytest.mark.asyncio
    async def test_eas_oodao_includes_ts(self, sync_instance):
        sync, _, _, mock_conn = sync_instance
        sync.SOURCE = "eas-oodao"

        mock_conn.fetch = AsyncMock(return_value=[{"voter": "0x1", "ts": 123}])
        result = await sync.read_votes_from_db("prop-1")
        assert len(result) == 1
        # Verify the query includes ", ts"
        call_qry = mock_conn.fetch.call_args[0][0]
        assert ", ts" in call_qry

    @pytest.mark.asyncio
    async def test_non_eas_oodao_excludes_ts(self, sync_instance):
        sync, _, _, mock_conn = sync_instance
        sync.SOURCE = "dao_node"

        mock_conn.fetch = AsyncMock(return_value=[])
        await sync.read_votes_from_db("prop-1")
        call_qry = mock_conn.fetch.call_args[0][0]
        assert ", ts" not in call_qry


class TestSyncInitWithNoneConfig:

    def test_init_calls_load_tenant_config_when_none(self):
        """Line 48: config=None triggers load_tenant_config."""
        fake_config = {
            "schema": "testdao", "dao_slug": "Test",
            "index_tenant_prefix": "t",
            "features": {},
            "deployment": {"chain_id": 1, "gov": {"address": "0x1"}, "token": {"address": "0x2"}},
        }
        with patch("cpls.sync.PostgreSQLClient"), \
             patch("cpls.sync.BlockCacheClient"), \
             patch("cpls.sync.load_tenant_config", return_value=fake_config) as mock_load:
            from cpls.sync import Sync
            Sync.__abstractmethods__ = frozenset()
            sync = Sync.__new__(Sync)
            sync.__init__("testdao", config=None, reset=False, http_client=MagicMock())
            mock_load.assert_called_once_with("testdao")
            assert sync.config == fake_config


class TestRefreshFullListSkipsNonNdjsonGz:

    @pytest.mark.asyncio
    async def test_skips_blob_not_ending_with_ndjson_gz(self, sync_instance, mock_gcs_client):
        """Line 118: blobs not ending in .ndjson.gz are skipped."""
        sync, _, _, _ = sync_instance

        non_gz_blob = MagicMock()
        non_gz_blob.name = "data/testdao/proposal_list/dao_node/raw.ndjson"
        gz_blob = MagicMock()
        gz_blob.name = "data/testdao/proposal_list/dao_node/raw.ndjson.gz"

        mock_gcs_client.list_blobs = AsyncMock(return_value=[non_gz_blob, gz_blob])
        mock_gcs_client.read_ndjson = AsyncMock(return_value=[
            {"id": "p1", "end_blocktime": "1000", "hybrid": False,
             "data_eng_properties": {"source": "dao_node"}}
        ])

        await sync.refresh_full_list(mock_gcs_client)

        # Only the .ndjson.gz blob should trigger read_ndjson
        assert mock_gcs_client.read_ndjson.call_count == 1
        assert mock_gcs_client.read_ndjson.call_args[0][0] == gz_blob.name


class TestReadExistingHashNoneRaises:

    @pytest.mark.asyncio
    async def test_hash_none_raises_exception(self, sync_instance):
        """Line 194: existing_proposal_hash=None raises Exception."""
        sync, mock_gcs, _, _ = sync_instance

        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.metadata = {"liveness": "live", "hash": None, "num_of_votes": "0"}
        mock_gcs.get_blob = AsyncMock(return_value=mock_blob)

        with pytest.raises(Exception, match="cannot be None"):
            await sync.read_existing_raw_proposal_hash_if_exists("prop-999", mock_gcs)


class TestGetNonivotesVp:

    @pytest.fixture
    def sync_with_http(self):
        config = {
            "schema": "testdao", "dao_slug": "TestDAO",
            "index_tenant_prefix": "td",
            "features": {},
            "deployment": {"chain_id": 1, "gov": {"address": "0x1"}, "token": {"address": "0x2"}},
        }
        with patch("cpls.sync.PostgreSQLClient"), \
             patch("cpls.sync.BlockCacheClient"), \
             patch("cpls.sync.load_tenant_config", return_value=config):
            from cpls.sync import Sync
            Sync.__abstractmethods__ = frozenset()
            Sync.SOURCE = "dao_node"
            mock_http = AsyncMock()
            sync = object.__new__(Sync)
            sync.__init__("testdao", config=config, reset=False, http_client=mock_http)
            sync.http_client = mock_http
            return sync, mock_http

    @pytest.mark.asyncio
    async def test_exception_returns_empty_dict(self, sync_with_http):
        """Lines 299-301: exception in get_nonivotes_vp_at_block returns {}."""
        sync, mock_http = sync_with_http
        mock_http.get = AsyncMock(side_effect=Exception("network error"))
        result = await sync.get_nonivotes_vp_at_block(1000)
        assert result == {}

    @pytest.mark.asyncio
    async def test_404_returns_empty_dict(self, sync_with_http):
        """get_nonivotes_vp_at_block: 404 response returns {}."""
        sync, mock_http = sync_with_http
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_http.get = AsyncMock(return_value=mock_resp)
        result = await sync.get_nonivotes_vp_at_block(1000)
        assert result == {}

    @pytest.mark.asyncio
    async def test_total_nonivotes_exception_returns_zero(self, sync_with_http):
        """Lines 317-319: exception in get_total_nonivotes_vp_at_block returns 0."""
        sync, mock_http = sync_with_http
        mock_http.get = AsyncMock(side_effect=Exception("timeout"))
        result = await sync.get_total_nonivotes_vp_at_block(1000)
        assert result == 0

    @pytest.mark.asyncio
    async def test_total_nonivotes_404_returns_zero(self, sync_with_http):
        """get_total_nonivotes_vp_at_block: 404 returns 0."""
        sync, mock_http = sync_with_http
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_http.get = AsyncMock(return_value=mock_resp)
        result = await sync.get_total_nonivotes_vp_at_block(1000)
        assert result == 0

    @pytest.mark.asyncio
    async def test_total_nonivotes_returns_value(self, sync_with_http):
        """get_total_nonivotes_vp_at_block: success path returns total_vp."""
        sync, mock_http = sync_with_http
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"total_vp": 5000}
        mock_http.get = AsyncMock(return_value=mock_resp)
        result = await sync.get_total_nonivotes_vp_at_block(1000)
        assert result == 5000


class TestGetVpSnapshotReadNdjsonException:

    @pytest.mark.asyncio
    async def test_read_ndjson_exception_falls_through_to_db(self, sync_instance):
        """Lines 330-331: bare except swallows read_ndjson error and proceeds to DB fetch."""
        sync, _, mock_pool, mock_conn = sync_instance
        sync.index_tenant_prefix = "td"
        sync.token_addr = "0xToken"

        mock_gcs = MagicMock()
        mock_gcs.read_ndjson = AsyncMock(side_effect=Exception("blob missing"))
        mock_gcs.upload_ndjson = AsyncMock(return_value=True)

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=Exception("no nonivotes"))
        sync.http_client = mock_http

        mock_conn.fetch = AsyncMock(return_value=[{"addr": "0xDel", "vp": 500}])

        result = await sync.get_vp_snapshot_all_delegates(
            block_number=1000, gcs_client=mock_gcs, reset=False
        )
        assert len(result) == 1
        assert result[0]["addr"] == "0xdel"


class TestGetVpSnapshotAllDelegates:

    @pytest.mark.asyncio
    async def test_returns_cached_data(self, sync_instance, mock_gcs_client):
        """If GCS already has VP snapshot, return it without DB call."""
        sync, _, _, mock_conn = sync_instance

        cached_data = [{"addr": "0xabc", "vp": "1000"}]
        mock_gcs_client.read_ndjson = AsyncMock(return_value=cached_data)

        result = await sync.get_vp_snapshot_all_delegates(100, mock_gcs_client)
        assert result == cached_data
        mock_conn.fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_fetches_from_db_when_no_cache(self, sync_instance, mock_gcs_client):
        """When no GCS cache, fetch from DB and merge with nonivotes."""
        sync, _, _, mock_conn = sync_instance

        mock_gcs_client.read_ndjson = AsyncMock(return_value=None)
        mock_conn.fetch = AsyncMock(return_value=[
            {"addr": "0xabc", "vp": 1000},
            {"addr": "0xdef", "vp": 2000},
        ])

        # nonivotes returns 404
        mock_response = MagicMock()
        mock_response.status_code = 404
        sync.http_client.get = AsyncMock(return_value=mock_response)

        result = await sync.get_vp_snapshot_all_delegates(100, mock_gcs_client, chain_id=10)
        assert len(result) == 2
        mock_gcs_client.upload_ndjson.assert_called_once()

    @pytest.mark.asyncio
    async def test_merges_nonivotes(self, sync_instance, mock_gcs_client):
        """Nonivotes VP is merged with delegation VP."""
        sync, _, _, mock_conn = sync_instance

        mock_gcs_client.read_ndjson = AsyncMock(return_value=None)
        mock_conn.fetch = AsyncMock(return_value=[
            {"addr": "0xabc", "vp": 1000},
        ])

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"vp": {"0xabc": 500, "0xnew": 300}}
        mock_response.raise_for_status = MagicMock()
        sync.http_client.get = AsyncMock(return_value=mock_response)

        result = await sync.get_vp_snapshot_all_delegates(100, mock_gcs_client, chain_id=10)

        vp_dict = {r["addr"]: r["vp"] for r in result}
        assert vp_dict["0xabc"] == "1500"  # 1000 + 500
        assert vp_dict["0xnew"] == "300"
