import sys
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime

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
