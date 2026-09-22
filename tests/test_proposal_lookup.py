import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, AsyncMock

from cpls.proposal_lookup import (
    PROPOSAL_SOURCES,
    blob_metadata,
    proposal_blob_name,
    lookup_proposal,
    is_valid_tenant,
    is_valid_proposal_id,
)


def _mock_blob(exists=True, metadata=None, **attrs):
    blob = MagicMock()
    blob.exists = MagicMock(return_value=exists)
    blob.reload = MagicMock()
    blob.metadata = metadata if metadata is not None else {
        "proposal_id": "123", "liveness": "live", "source": "dao_node", "hash": "abc", "num_of_votes": "7",
    }
    defaults = dict(
        generation=5, metageneration=2, size=1234, content_type="application/gzip",
        content_encoding=None, cache_control="public, max-age=60", md5_hash="md5==", etag="etag1",
        time_created=datetime(2025, 1, 1, tzinfo=timezone.utc),
        updated=datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
    )
    defaults.update(attrs)
    for k, v in defaults.items():
        setattr(blob, k, v)
    return blob


class TestConstants:

    def test_sources_match_sync_classes(self):
        from cpls.sync_daonode import DaoNodeSync
        from cpls.sync_snapshot import SnapshotSync
        from cpls.sync_eas_atlas import EASAtlasSync
        from cpls.sync_eas_oodao import EASOoDaoSync
        expected = {DaoNodeSync.SOURCE, SnapshotSync.SOURCE, EASAtlasSync.SOURCE, EASOoDaoSync.SOURCE}
        assert set(PROPOSAL_SOURCES) == expected
        assert len(PROPOSAL_SOURCES) == len(expected)

    def test_blob_name_matches_sync_formula(self):
        assert proposal_blob_name("optimism", "dao_node", "42") == "data/optimism/proposal/dao_node/raw/42.json.gz"


class TestValidation:

    @pytest.mark.parametrize("value,ok", [
        ("optimism", True), ("ens", True), ("my-dao_2", True),
        ("", False), ("Optimism", False), ("../x", False), ("a/b", False), ("a b", False),
    ])
    def test_tenant(self, value, ok):
        assert is_valid_tenant(value) is ok

    @pytest.mark.parametrize("value,ok", [
        ("104658512477211447238723406913978051219515164565395855005009394415444207632959", True),
        ("0xabc123DEF", True), ("prop-1_2", True),
        ("", False), ("../x", False), ("a/b", False), ("a.json", False), ("1 2", False),
    ])
    def test_proposal_id(self, value, ok):
        assert is_valid_proposal_id(value) is ok


class TestBlobMetadata:

    def test_shape_and_datetime_conversion(self):
        md = blob_metadata(_mock_blob())
        assert md["generation"] == 5
        assert md["metageneration"] == 2
        assert md["size"] == 1234
        assert md["content_type"] == "application/gzip"
        assert md["cache_control"] == "public, max-age=60"
        assert md["updated"] == "2025-01-02T03:04:05+00:00"
        assert md["time_created"] == "2025-01-01T00:00:00+00:00"
        assert md["custom_metadata"]["liveness"] == "live"
        assert md["custom_metadata"]["hash"] == "abc"

    def test_none_metadata_becomes_empty_dict(self):
        md = blob_metadata(_mock_blob(metadata={}))
        assert md["custom_metadata"] == {}
        blob = _mock_blob()
        blob.metadata = None
        assert blob_metadata(blob)["custom_metadata"] == {}


class TestLookupProposal:

    async def test_probes_all_sources_and_returns_matches(self, mock_gcs_client):
        found_name = proposal_blob_name("optimism", "dao_node", "1")

        async def get_blob(name):
            return _mock_blob(exists=(name == found_name))

        mock_gcs_client.get_blob = AsyncMock(side_effect=get_blob)
        mock_gcs_client.read_dict = AsyncMock(return_value={"id": "1", "title": "T"})

        results = await lookup_proposal(mock_gcs_client, "optimism", "1")

        assert mock_gcs_client.get_blob.await_count == len(PROPOSAL_SOURCES)
        assert len(results) == 1
        entry = results[0]
        assert entry["source"] == "dao_node"
        assert entry["blob_name"] == found_name
        assert entry["metadata"]["generation"] == 5
        assert entry["proposal"] == {"id": "1", "title": "T"}
        assert entry["error"] is None
        mock_gcs_client.read_dict.assert_awaited_once_with(found_name)

    async def test_source_filter_limits_probes(self, mock_gcs_client):
        mock_gcs_client.get_blob = AsyncMock(return_value=_mock_blob(exists=False))
        results = await lookup_proposal(mock_gcs_client, "optimism", "1", ["snapshot"])
        assert results == []
        mock_gcs_client.get_blob.assert_awaited_once_with(proposal_blob_name("optimism", "snapshot", "1"))
        mock_gcs_client.read_dict.assert_not_awaited()

    async def test_read_failure_is_captured_per_entry(self, mock_gcs_client):
        mock_gcs_client.get_blob = AsyncMock(return_value=_mock_blob(exists=True))
        mock_gcs_client.read_dict = AsyncMock(side_effect=Exception("gunzip failed"))
        results = await lookup_proposal(mock_gcs_client, "optimism", "1", ["dao_node"])
        assert len(results) == 1
        assert results[0]["proposal"] is None
        assert "gunzip failed" in results[0]["error"]
        assert results[0]["metadata"]["generation"] == 5

    async def test_get_blob_failure_propagates(self, mock_gcs_client):
        mock_gcs_client.get_blob = AsyncMock(side_effect=Exception("gcs down"))
        with pytest.raises(Exception, match="gcs down"):
            await lookup_proposal(mock_gcs_client, "optimism", "1", ["dao_node"])

    async def test_null_body_is_kept(self, mock_gcs_client):
        mock_gcs_client.get_blob = AsyncMock(return_value=_mock_blob(exists=True))
        mock_gcs_client.read_dict = AsyncMock(return_value=None)
        results = await lookup_proposal(mock_gcs_client, "optimism", "1", ["snapshot"])
        assert results[0]["proposal"] is None
        assert results[0]["error"] is None
