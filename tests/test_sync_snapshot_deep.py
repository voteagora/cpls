"""
Deep coverage tests for SnapshotSync and SnapshotGraphQLClient:
GraphQL fetching, vote pagination, and SnapshotSync init.
"""

import pytest
import time
from unittest.mock import AsyncMock, MagicMock, patch


class TestSnapshotGraphQLClient:

    @pytest.fixture
    def sgql_client(self):
        mock_http = AsyncMock()
        from cpls.sync_snapshot import SnapshotGraphQLClient
        client = SnapshotGraphQLClient("ens", http_client=mock_http)
        return client, mock_http

    @pytest.mark.asyncio
    async def test_get_proposals(self, sgql_client):
        client, mock_http = sgql_client
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"data": {"items": [
            {"id": "p1", "state": "active"},
            {"id": "p2", "state": "closed"},
        ]}}
        mock_http.post = AsyncMock(return_value=mock_resp)

        result = await client.get_proposals()
        assert len(result) == 2
        assert result[0]["id"] == "p1"

    @pytest.mark.asyncio
    async def test_get_votes_single_page(self, sgql_client):
        client, mock_http = sgql_client
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"data": {"items": [
            {"id": "v1", "voter": "0xA", "vp": 100},
        ]}}
        mock_http.post = AsyncMock(return_value=mock_resp)

        result = await client.get_votes("p1", 0)
        assert len(result) == 1
        assert result[0]["voter"] == "0xA"

    @pytest.mark.asyncio
    async def test_get_all_votes_paginates(self, sgql_client):
        """Paginate until we get less than page_size results."""
        client, mock_http = sgql_client
        client.page_size = 2

        page0_resp = MagicMock()
        page0_resp.json.return_value = {"data": {"items": [
            {"id": "v1", "voter": "0xA"}, {"id": "v2", "voter": "0xB"},
        ]}}
        page1_resp = MagicMock()
        page1_resp.json.return_value = {"data": {"items": [
            {"id": "v3", "voter": "0xC"},
        ]}}
        mock_http.post = AsyncMock(side_effect=[page0_resp, page1_resp])

        result = await client.get_all_votes("p1")
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_get_all_votes_none_page_breaks(self, sgql_client):
        """Pagination stops if a page returns None."""
        client, mock_http = sgql_client

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"data": {"items": None}}
        mock_http.post = AsyncMock(return_value=mock_resp)

        result = await client.get_all_votes("p1")
        assert result == []

    @pytest.mark.asyncio
    async def test_space_mapping(self):
        from cpls.sync_snapshot import SnapshotGraphQLClient
        for tenant, expected in [("ens", "ens.eth"), ("uniswap", "uniswapgovernance.eth"),
                                  ("derive", "derivexyz.eth"), ("etherfi", "etherfi-dao.eth")]:
            client = SnapshotGraphQLClient(tenant, http_client=AsyncMock())
            assert client.space == expected


class TestSnapshotSyncInit:

    def test_init_sets_attributes(self):
        config = {
            "schema": "ens", "dao_slug": "ENS",
            "index_tenant_prefix": "en",
            "features": {"snapshot_proposals": True},
            "deployment": {"chain_id": 1,
                           "gov": {"address": "0xGov"},
                           "token": {"address": "0xToken"}},
        }
        with patch("cpls.sync.PostgreSQLClient"), \
             patch("cpls.sync.BlockCacheClient"), \
             patch("cpls.sync.load_tenant_config", return_value=config):
            from cpls.sync_snapshot import SnapshotSync
            sync = SnapshotSync("ens", config, reset=False, http_client=AsyncMock())
            assert sync.index_tenant_prefix == "en"
            assert sync.token_addr == "0xToken"
            assert sync.dao_slug == "ENS"
            assert sync.SOURCE == "snapshot"
