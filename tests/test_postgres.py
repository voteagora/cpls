"""
Tests for PostgreSQLClient: connect (pool creation) and disconnect.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


class TestPostgreSQLClient:

    @pytest.fixture
    def pg_client(self):
        from cpls.postgres import PostgreSQLClient
        return PostgreSQLClient("postgres://user:pass@localhost/testdb")

    def test_init_stores_url(self, pg_client):
        assert pg_client.database_url == "postgres://user:pass@localhost/testdb"
        assert pg_client.pool is None

    @pytest.mark.asyncio
    async def test_connect_creates_pool(self, pg_client):
        mock_pool = MagicMock()
        with patch("cpls.postgres.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
            mock_create.return_value = mock_pool
            result = await pg_client.connect()
            assert result is mock_pool
            mock_create.assert_called_once_with(
                "postgres://user:pass@localhost/testdb",
                min_size=1,
                max_size=10
            )

    @pytest.mark.asyncio
    async def test_connect_reuses_existing_pool(self, pg_client):
        existing_pool = MagicMock()
        pg_client.pool = existing_pool
        with patch("cpls.postgres.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
            result = await pg_client.connect()
            assert result is existing_pool
            mock_create.assert_not_called()

    @pytest.mark.asyncio
    async def test_disconnect_closes_pool(self, pg_client):
        mock_pool = MagicMock()
        mock_pool.close = AsyncMock()
        pg_client.pool = mock_pool
        await pg_client.disconnect()
        mock_pool.close.assert_called_once()
        assert pg_client.pool is None

    @pytest.mark.asyncio
    async def test_disconnect_noop_when_no_pool(self, pg_client):
        pg_client.pool = None
        await pg_client.disconnect()
        assert pg_client.pool is None
