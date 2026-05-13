import pytest
from unittest.mock import AsyncMock, MagicMock

def pytest_addoption(parser):
    parser.addoption(
        "--update-fixtures",
        action="store_true",
        default=False,
        help="Regenerate golden proposal fixtures from live GCS data",
    )


@pytest.fixture
def update_fixtures(request):
    return request.config.getoption("--update-fixtures")


@pytest.fixture
def sample_tenant_config():
    """A minimal tenant config matching the YAML structure."""
    return {
        "schema": "testdao",
        "dao_slug": "TestDAO",
        "index_tenant_prefix": "td",
        "features": {
            "oodao": False,
            "snapshot_proposals": False,
            "dao_node_proposals": True,
        },
        "deployment": {
            "chain_id": 10,
            "gov": {"address": "0xcDF27F107725988f2261Ce2256bDfCdE8B382B10"},
            "token": {"address": "0x4200000000000000000000000000000000000042"},
        },
    }


@pytest.fixture
def sample_proposal():
    """A minimal proposal dict for testing."""
    return {
        "id": "12345",
        "title": "Test Proposal",
        "description": "# Test Proposal\n\nThis is a test.",
        "proposer": "0xAbC1230000000000000000000000000000000001",
        "start_block": 100,
        "end_block": 200,
        "start_blocktime": 1700000000,
        "end_blocktime": 1700100000,
        "num_of_votes": 5,
        "data_eng_properties": {
            "liveness": "live",
            "source": "dao_node",
            "hash": "abc123",
        },
    }


@pytest.fixture
def mock_gcs_client():
    """Mock GCSClient with all async methods."""
    client = MagicMock()
    client.upload_dict = AsyncMock(return_value=True)
    client.upload_ndjson = AsyncMock(return_value=True)
    client.read_dict = AsyncMock(return_value=None)
    client.read_ndjson = AsyncMock(return_value=None)
    client.list_blobs = AsyncMock(return_value=[])
    client.get_blob = AsyncMock()
    client.safe_upload_job_result = AsyncMock()
    return client


@pytest.fixture
def mock_http_client():
    """Mock httpx.AsyncClient."""
    client = AsyncMock()
    return client


@pytest.fixture
def mock_pg_pool():
    """Mock asyncpg connection pool."""
    pool = MagicMock()
    connection = AsyncMock()
    pool.acquire = MagicMock(return_value=connection)
    # Make it work as async context manager
    connection.__aenter__ = AsyncMock(return_value=connection)
    connection.__aexit__ = AsyncMock(return_value=False)
    connection.fetch = AsyncMock(return_value=[])
    connection.fetchrow = AsyncMock(return_value=None)
    return pool
