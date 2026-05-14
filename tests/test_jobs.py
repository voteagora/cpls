import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from cpls.jobs import Job, JobRequest, JobStatus, JobQueue


class TestJobModel:

    def test_defaults(self):
        job = Job(id="abc", type="scheduled", payload={"key": "val"}, created_at=datetime.now())
        assert job.status == JobStatus.PENDING
        assert job.started_at is None
        assert job.completed_at is None
        assert job.error is None
        assert job.stats is None

    def test_all_fields(self):
        now = datetime.now()
        job = Job(
            id="abc",
            type="external",
            payload={"a": 1},
            status=JobStatus.COMPLETED,
            created_at=now,
            started_at=now,
            completed_at=now,
            error=None,
            stats={"refreshed": 5},
        )
        assert job.status == JobStatus.COMPLETED
        assert job.stats == {"refreshed": 5}


class TestJobRequest:

    def test_valid(self):
        req = JobRequest(type="scheduled", payload={"infra_dao_slug": "optimism"})
        assert req.type == "scheduled"

    def test_payload_required(self):
        with pytest.raises(Exception):
            JobRequest(type="scheduled")


class TestJobStatus:

    def test_values(self):
        assert JobStatus.PENDING == "pending"
        assert JobStatus.PROCESSING == "processing"
        assert JobStatus.COMPLETED == "completed"
        assert JobStatus.FAILED == "failed"
        assert JobStatus.SKIPPED == "skipped"


class TestJobQueue:

    @pytest.fixture
    def queue(self):
        with patch("cpls.jobs.create_http_client") as mock_http:
            mock_http.return_value = AsyncMock()
            q = JobQueue(num_workers=2)
            return q

    @pytest.mark.asyncio
    async def test_add_job(self, queue):
        job_id = await queue.add_job("scheduled", {"infra_dao_slug": "optimism", "message": "test"})
        assert job_id in queue.jobs
        assert queue.jobs[job_id].status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_add_job_skipped_when_locked(self, queue):
        # Acquire the lock for this DAO
        dao_lock = queue._get_dao_lock("optimism")
        await dao_lock.acquire()

        try:
            job_id = await queue.add_job("scheduled", {"infra_dao_slug": "optimism", "message": "test"})
            assert queue.jobs[job_id].status == JobStatus.SKIPPED
            assert "Queue is full" in queue.jobs[job_id].error
        finally:
            dao_lock.release()

    @pytest.mark.asyncio
    async def test_get_dao_lock_returns_same_lock(self, queue):
        lock1 = queue._get_dao_lock("optimism")
        lock2 = queue._get_dao_lock("optimism")
        assert lock1 is lock2

    @pytest.mark.asyncio
    async def test_different_dao_different_lock(self, queue):
        lock1 = queue._get_dao_lock("optimism")
        lock2 = queue._get_dao_lock("ens")
        assert lock1 is not lock2

    def test_get_all_jobs_sorted(self, queue):
        now = datetime.now()
        job1 = Job(id="a", type="t", payload={}, created_at=datetime(2024, 1, 1))
        job2 = Job(id="b", type="t", payload={}, created_at=datetime(2024, 6, 1))
        queue.jobs = {"a": job1, "b": job2}
        result = queue.get_all_jobs()
        assert result[0].id == "b"  # Most recent first
        assert result[1].id == "a"

    def test_stop(self, queue):
        queue.processing = True
        mock_task = MagicMock()
        queue.worker_tasks = [mock_task]
        queue.stop()
        assert queue.processing is False
        mock_task.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_cancelled_error_handled(self, queue):
        """Lines 185-186: CancelledError during process_jobs gather is caught and printed."""
        mock_gcs = MagicMock()
        with patch("cpls.jobs.asyncio.gather", side_effect=asyncio.CancelledError), \
             patch("cpls.jobs.asyncio.create_task", return_value=MagicMock()), \
             patch("cpls.jobs.asyncio.sleep", new_callable=AsyncMock):
            await queue.process_jobs(mock_gcs)
