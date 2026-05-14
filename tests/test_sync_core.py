import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from cpls.sync import Sync, SkipProposal, json_hash


class TestOverwriteProposal:

    @pytest.fixture
    def sync_instance(self):
        with patch("cpls.sync.PostgreSQLClient"), \
             patch("cpls.sync.BlockCacheClient"), \
             patch("cpls.sync.load_tenant_config"):
            s = Sync.__new__(Sync)
            s.infra_dao_slug = "optimism"
            s.SOURCE = "dao_node"
            s.config = {}
            s.reset = False
            s.pg = MagicMock()
            s.bc = MagicMock()
            return s

    @pytest.mark.asyncio
    @patch("cpls.sync.ENVIRONMENT", "prod")
    @patch("cpls.sync.SCHEDULER_INTERVAL_MINUTES", 10)
    async def test_overwrites_with_correct_metadata(self, sync_instance, mock_gcs_client):
        proposal = {"id": "123", "title": "Test", "num_of_votes": 10}
        await sync_instance.overwrite_proposal(proposal, "hash123", "live", mock_gcs_client)

        mock_gcs_client.upload_dict.assert_called_once()
        call_args = mock_gcs_client.upload_dict.call_args

        # Check the proposal was enriched with data_eng_properties
        uploaded_data = call_args[0][0]
        assert uploaded_data["data_eng_properties"]["liveness"] == "live"
        assert uploaded_data["data_eng_properties"]["source"] == "dao_node"
        assert uploaded_data["data_eng_properties"]["hash"] == "hash123"

        # Check metadata
        metadata = call_args[1]["metadata"]
        assert metadata["proposal_id"] == "123"
        assert metadata["liveness"] == "live"
        assert metadata["num_of_votes"] == 10

        # Check cache control for live/prod
        assert call_args[1]["cache_control"] == "public, max-age=300"

    @pytest.mark.asyncio
    async def test_raises_on_none_hash(self, sync_instance, mock_gcs_client):
        with pytest.raises(Exception, match="Proposal hash cannot be None"):
            await sync_instance.overwrite_proposal({"id": "1"}, None, "live", mock_gcs_client)


class TestReadExistingRawProposalHash:

    @pytest.fixture
    def sync_instance(self):
        with patch("cpls.sync.PostgreSQLClient"), \
             patch("cpls.sync.BlockCacheClient"), \
             patch("cpls.sync.load_tenant_config"):
            s = Sync.__new__(Sync)
            s.infra_dao_slug = "optimism"
            s.SOURCE = "dao_node"
            s.config = {}
            s.reset = False
            s.pg = MagicMock()
            s.bc = MagicMock()
            return s

    @pytest.mark.asyncio
    async def test_new_proposal_returns_defaults(self, sync_instance, mock_gcs_client):
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)

        blob, liveness, hash_, num_votes = await sync_instance.read_existing_raw_proposal_hash_if_exists("999", mock_gcs_client)
        assert liveness == "new"
        assert hash_ == "no-hash"
        assert num_votes == 0

    @pytest.mark.asyncio
    async def test_existing_live_proposal(self, sync_instance, mock_gcs_client):
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.metadata = {"liveness": "live", "hash": "abc123", "num_of_votes": "5"}
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)

        blob, liveness, hash_, num_votes = await sync_instance.read_existing_raw_proposal_hash_if_exists("123", mock_gcs_client)
        assert liveness == "live"
        assert hash_ == "abc123"
        assert num_votes == 5

    @pytest.mark.asyncio
    async def test_archived_proposal_skipped_without_reset(self, sync_instance, mock_gcs_client):
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.metadata = {"liveness": "archived", "hash": "abc", "num_of_votes": "0"}
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)

        with pytest.raises(SkipProposal, match="archived"):
            await sync_instance.read_existing_raw_proposal_hash_if_exists("123", mock_gcs_client)

    @pytest.mark.asyncio
    async def test_archived_proposal_not_skipped_with_reset(self, sync_instance, mock_gcs_client):
        sync_instance.reset = True
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_blob.metadata = {"liveness": "archived", "hash": "abc", "num_of_votes": "3"}
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)

        blob, liveness, hash_, num_votes = await sync_instance.read_existing_raw_proposal_hash_if_exists("123", mock_gcs_client)
        assert liveness == "archived"
        assert num_votes == 3

    @pytest.mark.asyncio
    async def test_existence_check_failure_skips(self, sync_instance, mock_gcs_client):
        mock_blob = MagicMock()
        mock_blob.exists.side_effect = Exception("GCS down")
        mock_gcs_client.get_blob = AsyncMock(return_value=mock_blob)

        with pytest.raises(SkipProposal, match="Existence check failed"):
            await sync_instance.read_existing_raw_proposal_hash_if_exists("123", mock_gcs_client)


class TestOverwriteVotes:

    @pytest.fixture
    def sync_instance(self):
        with patch("cpls.sync.PostgreSQLClient"), \
             patch("cpls.sync.BlockCacheClient"), \
             patch("cpls.sync.load_tenant_config"):
            s = Sync.__new__(Sync)
            s.infra_dao_slug = "optimism"
            s.SOURCE = "dao_node"
            s.pg = MagicMock()
            s.bc = MagicMock()
            return s

    @pytest.mark.asyncio
    async def test_uploads_votes(self, sync_instance, mock_gcs_client):
        votes = [{"voter": "0x1", "weight": "100"}]
        await sync_instance.overwrite_votes(votes, "prop-1", mock_gcs_client)
        mock_gcs_client.upload_ndjson.assert_called_once_with(
            votes, "data/optimism/votes/prop-1.ndjson.gz"
        )

    @pytest.mark.asyncio
    async def test_uploads_hasnt_voted(self, sync_instance, mock_gcs_client):
        hasnt_voted = [{"addr": "0x2", "vp": "500"}]
        await sync_instance.overwrite_hasnt_voted(hasnt_voted, "prop-1", mock_gcs_client)
        mock_gcs_client.upload_ndjson.assert_called_once_with(
            hasnt_voted, "data/optimism/hasnt_voted/prop-1.ndjson.gz"
        )


class TestGetVpSnapshotAllDelegates:

    @pytest.fixture
    def sync_instance(self):
        with patch("cpls.sync.PostgreSQLClient"), \
             patch("cpls.sync.BlockCacheClient"), \
             patch("cpls.sync.load_tenant_config"):
            s = Sync.__new__(Sync)
            s.infra_dao_slug = "optimism"
            s.SOURCE = "dao_node"
            s.index_tenant_prefix = "op"
            s.token_addr = "0x4200000000000000000000000000000000000042"
            s.config = {}
            s.reset = False
            s.pg = MagicMock()
            s.bc = MagicMock()
            s.http_client = AsyncMock()
            return s

    @pytest.mark.asyncio
    async def test_returns_cached_if_exists(self, sync_instance, mock_gcs_client):
        cached_data = [{"addr": "0xabc", "vp": "1000"}]
        mock_gcs_client.read_ndjson = AsyncMock(return_value=cached_data)

        result = await sync_instance.get_vp_snapshot_all_delegates(12345, mock_gcs_client)
        assert result == cached_data
        # Should not have called DB
        sync_instance.pg.connect.assert_not_called()

    @pytest.mark.asyncio
    async def test_merges_delegation_and_nonivotes(self, sync_instance, mock_gcs_client):
        mock_gcs_client.read_ndjson = AsyncMock(return_value=None)

        # Mock DB delegation data using a proper async context manager
        mock_conn = MagicMock()
        mock_conn.fetch = AsyncMock(return_value=[
            {"addr": "0xaaa", "vp": 1000},
            {"addr": "0xbbb", "vp": 2000},
        ])

        # Create a proper async context manager for pool.acquire()
        class FakeAcquire:
            async def __aenter__(self):
                return mock_conn
            async def __aexit__(self, *args):
                return False

        mock_pool = MagicMock()
        mock_pool.acquire.return_value = FakeAcquire()
        sync_instance.pg.connect = AsyncMock(return_value=mock_pool)

        # Mock nonivotes API - returns for one overlapping and one new addr
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"vp": {"0xaaa": "500", "0xccc": "300"}}
        sync_instance.http_client.get = AsyncMock(return_value=mock_response)

        result = await sync_instance.get_vp_snapshot_all_delegates(
            12345, mock_gcs_client, chain_id=10
        )

        result_dict = {r["addr"]: r["vp"] for r in result}
        assert result_dict["0xaaa"] == "1500"  # 1000 + 500 merged
        assert result_dict["0xbbb"] == "2000"  # delegation only
        assert result_dict["0xccc"] == "300"   # nonivotes only
