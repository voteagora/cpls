import sys
import json
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Create a TestClient with mocked scheduler and GCS.

    We must patch GCSClient before cpls.server is imported because the module
    instantiates GCSClient at the top level (which attempts to create dirs
    and connect to GCS).
    """
    # Patch GCSClient at the gcs module level before server import triggers it
    with patch("cpls.gcs.storage"), \
         patch("cpls.gcs.Path"), \
         patch("cpls.gcs.WRITE_TO_DISK", False):

        # Remove cached server module so re-import picks up our patches
        mods_to_remove = [k for k in sys.modules if k.startswith("cpls.server")]
        for mod in mods_to_remove:
            del sys.modules[mod]

        with patch("cpls.server.load_tenant_configs", return_value={}), \
             patch("cpls.server.INFRA_DAO_SLUGS", []), \
             patch("cpls.server.scheduler") as mock_scheduler, \
             patch("cpls.server.job_queue") as mock_queue:

            mock_scheduler.start = MagicMock()
            mock_scheduler.shutdown = MagicMock()
            mock_scheduler.add_job = MagicMock()
            mock_queue.process_jobs = AsyncMock()
            mock_queue.stop = MagicMock()
            mock_queue.queue = MagicMock()
            mock_queue.queue.qsize.return_value = 0
            mock_queue.jobs = {}
            mock_queue.current_job = None

            from cpls.server import app
            with TestClient(app) as c:
                yield c, mock_queue


class TestHealthEndpoint:

    def test_health_returns_200(self, client):
        c, _ = client
        resp = c.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert "queue_size" in data
        assert "total_jobs" in data

    def test_health_shows_queue_size(self, client):
        c, mock_queue = client
        mock_queue.queue.qsize.return_value = 5
        mock_queue.jobs = {"a": 1, "b": 2, "c": 3}
        resp = c.get("/health")
        data = resp.json()
        assert data["queue_size"] == 5
        assert data["total_jobs"] == 3


class TestGetJobEndpoint:

    def test_get_nonexistent_job_404(self, client):
        c, _ = client
        resp = c.get("/jobs/nonexistent-id")
        assert resp.status_code == 404

    def test_get_existing_job(self, client):
        c, mock_queue = client
        from cpls.jobs import Job, JobStatus
        job = Job(
            id="test-123",
            type="scheduled",
            payload={"infra_dao_slug": "optimism"},
            status=JobStatus.COMPLETED,
            created_at=datetime(2024, 1, 15, 10, 30),
            started_at=datetime(2024, 1, 15, 10, 30, 1),
            completed_at=datetime(2024, 1, 15, 10, 35),
        )
        mock_queue.jobs = {"test-123": job}
        resp = c.get("/jobs/test-123")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == "test-123"
        assert data["type"] == "scheduled"
        assert data["status"] == "completed"
        assert data["error"] is None


class TestDashboardEndpoint:

    def test_dashboard_returns_html(self, client):
        c, mock_queue = client
        mock_queue.get_all_jobs.return_value = []
        resp = c.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Job Processing Dashboard" in resp.text


class TestCreateJobEndpoint:

    def test_create_job(self, client):
        c, mock_queue = client
        mock_queue.add_job = AsyncMock(return_value="new-job-id")
        resp = c.post("/jobs", json={
            "type": "external",
            "payload": {"infra_dao_slug": "ens", "logic": "refresh_list"}
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["job_id"] == "new-job-id"
        assert data["status"] == "queued"


DAO_NODE_FIXTURE = (
    Path(__file__).parent / "fixtures" / "proposals" / "optimism" / "dao_node"
    / "104658512477211447238723406913978051219515164565395855005009394415444207632959.json"
)
PROPOSAL_ID = "104658512477211447238723406913978051219515164565395855005009394415444207632959"


def _mock_blob(exists=True):
    blob = MagicMock()
    blob.exists = MagicMock(return_value=exists)
    blob.reload = MagicMock()
    blob.metadata = {"proposal_id": PROPOSAL_ID, "liveness": "live", "source": "dao_node", "hash": "h", "num_of_votes": "4753"}
    blob.generation = 777
    blob.metageneration = 1
    blob.size = 2048
    blob.content_type = "application/gzip"
    blob.content_encoding = None
    blob.cache_control = "public, max-age=60"
    blob.md5_hash = "md5=="
    blob.etag = "e1"
    blob.time_created = datetime(2025, 7, 24, tzinfo=timezone.utc)
    blob.updated = datetime(2025, 8, 1, 12, 0, 0, tzinfo=timezone.utc)
    return blob


class TestProposalLookupEndpoint:

    def test_blank_form(self, client):
        c, _ = client
        resp = c.get("/proposals")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Proposal Lookup" in resp.text
        assert 'name="proposal_id"' in resp.text
        assert "No proposal found" not in resp.text

    def test_only_tenant_given_renders_form(self, client):
        c, _ = client
        with patch("cpls.server.gcs_client") as gcs:
            resp = c.get("/proposals", params={"tenant": "optimism"})
            assert resp.status_code == 200
            gcs.get_blob.assert_not_called()

    def test_found_in_one_source(self, client):
        c, _ = client
        proposal = json.loads(DAO_NODE_FIXTURE.read_text())
        dao_node_name = f"data/optimism/proposal/dao_node/raw/{PROPOSAL_ID}.json.gz"

        async def get_blob(name):
            return _mock_blob(exists=(name == dao_node_name))

        with patch("cpls.server.gcs_client") as gcs:
            gcs.get_blob = AsyncMock(side_effect=get_blob)
            gcs.read_dict = AsyncMock(return_value=proposal)
            resp = c.get("/proposals", params={"tenant": "optimism", "proposal_id": PROPOSAL_ID})

        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert gcs.get_blob.await_count == 4
        gcs.read_dict.assert_awaited_once_with(dao_node_name)
        assert resp.text.count('<section class="card result"') == 1
        assert "Source: dao_node" in resp.text
        assert "Developer Advisory Board Election: Members" in resp.text
        assert "777" in resp.text
        assert "2025-08-01T12:00:00+00:00" in resp.text
        assert "live" in resp.text

    def test_source_param_narrows_lookup(self, client):
        c, _ = client
        with patch("cpls.server.gcs_client") as gcs:
            gcs.get_blob = AsyncMock(return_value=_mock_blob(exists=False))
            resp = c.get("/proposals", params={"tenant": "optimism", "proposal_id": "0xabc", "source": "snapshot"})
        assert resp.status_code == 404
        gcs.get_blob.assert_awaited_once_with("data/optimism/proposal/snapshot/raw/0xabc.json.gz")
        assert "No proposal found" in resp.text
        assert '<option value="snapshot" selected>' in resp.text

    def test_not_found_anywhere_is_404_html(self, client):
        c, _ = client
        with patch("cpls.server.gcs_client") as gcs:
            gcs.get_blob = AsyncMock(return_value=_mock_blob(exists=False))
            resp = c.get("/proposals", params={"tenant": "optimism", "proposal_id": "123"})
        assert resp.status_code == 404
        assert "text/html" in resp.headers["content-type"]
        assert "No proposal found" in resp.text
        assert gcs.get_blob.await_count == 4

    def test_invalid_source_is_400_html(self, client):
        c, _ = client
        with patch("cpls.server.gcs_client") as gcs:
            resp = c.get("/proposals", params={"tenant": "optimism", "proposal_id": "123", "source": "bogus"})
            gcs.get_blob.assert_not_called()
        assert resp.status_code == 400
        assert "text/html" in resp.headers["content-type"]
        assert "Unknown source" in resp.text
        assert "bogus" in resp.text

    def test_invalid_proposal_id_is_400(self, client):
        c, _ = client
        with patch("cpls.server.gcs_client") as gcs:
            resp = c.get("/proposals", params={"tenant": "optimism", "proposal_id": "../x"})
            gcs.get_blob.assert_not_called()
        assert resp.status_code == 400
        assert "Invalid proposal ID" in resp.text

    def test_invalid_tenant_is_400(self, client):
        c, _ = client
        with patch("cpls.server.gcs_client") as gcs:
            resp = c.get("/proposals", params={"tenant": "opt/imism", "proposal_id": "123"})
            gcs.get_blob.assert_not_called()
        assert resp.status_code == 400
        assert "Invalid tenant" in resp.text

    def test_gcs_failure_is_502(self, client):
        c, _ = client
        with patch("cpls.server.gcs_client") as gcs:
            gcs.get_blob = AsyncMock(side_effect=Exception("connection refused"))
            resp = c.get("/proposals", params={"tenant": "optimism", "proposal_id": "123"})
        assert resp.status_code == 502
        assert "GCS error" in resp.text
        assert "connection refused" in resp.text

    def test_body_read_failure_still_shows_metadata(self, client):
        c, _ = client
        with patch("cpls.server.gcs_client") as gcs:
            gcs.get_blob = AsyncMock(return_value=_mock_blob(exists=True))
            gcs.read_dict = AsyncMock(side_effect=Exception("bad gzip"))
            resp = c.get("/proposals", params={"tenant": "optimism", "proposal_id": "123", "source": "dao_node"})
        assert resp.status_code == 200
        assert "777" in resp.text
        assert "Could not read proposal body" in resp.text
        assert "bad gzip" in resp.text

    def test_whitespace_is_stripped(self, client):
        c, _ = client
        with patch("cpls.server.gcs_client") as gcs:
            gcs.get_blob = AsyncMock(return_value=_mock_blob(exists=False))
            resp = c.get("/proposals", params={"tenant": "  optimism ", "proposal_id": " 123 ", "source": "dao_node"})
        assert resp.status_code == 404
        gcs.get_blob.assert_awaited_once_with("data/optimism/proposal/dao_node/raw/123.json.gz")
