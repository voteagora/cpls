import pytest
from unittest.mock import patch, MagicMock
from cpls.sync import json_hash, to_eth_address, SkipProposal, Sync


class TestJsonHash:

    def test_deterministic(self):
        obj = {"b": 2, "a": 1}
        h1 = json_hash(obj)
        h2 = json_hash(obj)
        assert h1 == h2

    def test_key_order_independent(self):
        h1 = json_hash({"a": 1, "b": 2})
        h2 = json_hash({"b": 2, "a": 1})
        assert h1 == h2

    def test_different_objects_different_hash(self):
        h1 = json_hash({"a": 1})
        h2 = json_hash({"a": 2})
        assert h1 != h2

    def test_nested_objects(self):
        obj = {"outer": {"inner": [1, 2, 3]}}
        h = json_hash(obj)
        assert isinstance(h, str)
        assert len(h) == 64  # SHA-256 hex digest


class TestToEthAddress:

    def test_basic_conversion(self):
        # 32-byte hex with leading zeros
        hex_str = "0x000000000000000000000000d8da6bf26964af9d7eed9e03e53415d37aa96045"
        result = to_eth_address(hex_str)
        assert result == "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

    def test_already_20_bytes(self):
        addr = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045"
        result = to_eth_address(addr)
        assert result == "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"

    def test_without_0x_prefix(self):
        hex_str = "000000000000000000000000d8da6bf26964af9d7eed9e03e53415d37aa96045"
        result = to_eth_address(hex_str)
        assert result.startswith("0x")
        assert len(result) == 42


class TestSkipProposal:

    def test_str_format(self):
        exc = SkipProposal("content unchanged", "12345")
        assert str(exc) == "[PROP-12345] content unchanged"

    def test_attributes(self):
        exc = SkipProposal("some reason", "99")
        assert exc.reason == "some reason"
        assert exc.proposal_id == "99"

    def test_is_exception(self):
        with pytest.raises(SkipProposal):
            raise SkipProposal("test", "1")


class TestCalcCacheControl:

    @pytest.fixture
    def sync_instance(self):
        """Create a Sync instance with mocked dependencies."""
        with patch("cpls.sync.PostgreSQLClient"), \
             patch("cpls.sync.BlockCacheClient"), \
             patch("cpls.sync.load_tenant_config"):
            s = Sync.__new__(Sync)
            s.infra_dao_slug = "test"
            s.config = {}
            s.reset = False
            s.http_client = None
            s.pg = MagicMock()
            s.bc = MagicMock()
            return s

    @patch("cpls.sync.ENVIRONMENT", "prod")
    @patch("cpls.sync.SCHEDULER_INTERVAL_MINUTES", 10)
    def test_live_prod(self, sync_instance):
        result = sync_instance.calc_cache_control("live")
        assert result == "public, max-age=300"

    @patch("cpls.sync.ENVIRONMENT", "dev")
    @patch("cpls.sync.SCHEDULER_INTERVAL_MINUTES", 10)
    def test_live_dev(self, sync_instance):
        result = sync_instance.calc_cache_control("live")
        assert result == "public, max-age=100"

    @patch("cpls.sync.ENVIRONMENT", "prod")
    def test_archived_prod(self, sync_instance):
        result = sync_instance.calc_cache_control("archived")
        assert result == "public, max-age=31536000"

    @patch("cpls.sync.ENVIRONMENT", "dev")
    def test_archived_dev(self, sync_instance):
        result = sync_instance.calc_cache_control("archived")
        assert result == "public, max-age=120"

    @patch("cpls.sync.ENVIRONMENT", "prod")
    def test_unqualified_prod(self, sync_instance):
        result = sync_instance.calc_cache_control("unqualified")
        assert result == "public, max-age=31536000"

    def test_unknown_liveness_raises(self, sync_instance):
        with pytest.raises(Exception, match="Unknown liveness"):
            sync_instance.calc_cache_control("garbage")


class TestBlobNameHelpers:

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

    def test_proposal_blob_name(self, sync_instance):
        result = sync_instance.proposal_blob_name("12345")
        assert result == "data/optimism/proposal/dao_node/raw/12345.json.gz"

    def test_votes_blob_name(self, sync_instance):
        result = sync_instance.votes_blob_name("12345")
        assert result == "data/optimism/votes/12345.ndjson.gz"

    def test_hasnt_voted_blob_name(self, sync_instance):
        result = sync_instance.hasnt_voted_blob_name("12345")
        assert result == "data/optimism/hasnt_voted/12345.ndjson.gz"

    def test_vp_snapshot_blob_name(self, sync_instance):
        result = sync_instance.vp_snapshot_blob_name(99999)
        assert result == "data/optimism/vpsnapshot/dao_node/raw/99999.ndjson.gz"


class TestCheckExistingProposalHash:

    @pytest.fixture
    def sync_instance(self):
        with patch("cpls.sync.PostgreSQLClient"), \
             patch("cpls.sync.BlockCacheClient"), \
             patch("cpls.sync.load_tenant_config"):
            s = Sync.__new__(Sync)
            s.infra_dao_slug = "test"
            s.reset = False
            s.pg = MagicMock()
            s.bc = MagicMock()
            return s

    def test_hash_unchanged_raises_skip(self, sync_instance):
        proposal = {"id": "1", "data": "test"}
        current_hash = json_hash(proposal)
        with pytest.raises(SkipProposal, match="content state is unchanged"):
            sync_instance.check_existing_proposal_hash(proposal, current_hash)

    def test_hash_changed_returns_new_hash(self, sync_instance):
        proposal = {"id": "1", "data": "test"}
        result = sync_instance.check_existing_proposal_hash(proposal, "old-hash")
        assert result == json_hash(proposal)

    def test_reset_bypasses_skip(self, sync_instance):
        sync_instance.reset = True
        proposal = {"id": "1", "data": "test"}
        current_hash = json_hash(proposal)
        result = sync_instance.check_existing_proposal_hash(proposal, current_hash)
        assert result == current_hash
