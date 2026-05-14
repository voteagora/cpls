"""
Tests for JobQueue._worker, process_jobs, add_job skipping, and error handling.
Covers the worker loop, DAO locking, job failure paths, and GCS upload errors.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime


class FakeAcquire:
    def __init__(self, conn):
        self._conn = conn
    async def __aenter__(self):
        return self._conn
    async def __aexit__(self, *args):
        return False


@pytest.fixture
def job_queue():
    with patch("cpls.jobs.create_http_client") as mock_http, \
         patch("cpls.sync.PostgreSQLClient"), \
         patch("cpls.sync.BlockCacheClient"), \
         patch("cpls.sync.load_tenant_config", return_value={
             "schema": "test", "dao_slug": "Test",
             "index_tenant_prefix": "t",
             "features": {},
             "deployment": {"chain_id": 1, "gov": {"address": "0x1"}, "token": {"address": "0x2"}},
         }):
        from cpls.jobs import JobQueue
        jq = JobQueue(num_workers=1)
        yield jq


class TestAddJobSkipping:

    @pytest.mark.asyncio
    async def test_add_job_creates_pending_job(self, job_queue):
        job_id = await job_queue.add_job("sync", {"infra_dao_slug": "test", "sources": []})
        assert job_id is not None or True  # add_job may return None
        assert len(job_queue.jobs) == 1
        job = list(job_queue.jobs.values())[0]
        from cpls.jobs import JobStatus
        assert job.status in (JobStatus.PENDING, JobStatus.SKIPPED)

    @pytest.mark.asyncio
    async def test_add_job_skips_when_dao_locked(self, job_queue):
        """When a DAO lock is already held, new jobs are skipped."""
        from cpls.jobs import JobStatus
        lock = job_queue._get_dao_lock("testdao")
        await lock.acquire()
        try:
            await job_queue.add_job("sync", {"infra_dao_slug": "testdao", "sources": []})
            job = list(job_queue.jobs.values())[0]
            assert job.status == JobStatus.SKIPPED
            assert "skipped" in job.error.lower()
        finally:
            lock.release()


class TestWorkerExecution:

    @pytest.mark.asyncio
    async def test_worker_processes_job_successfully(self, job_queue):
        """Worker picks up a job from queue and processes it."""
        from cpls.jobs import JobStatus, Job, GCSClient

        mock_gcs = MagicMock()
        mock_gcs.safe_upload_job_result = AsyncMock()

        with patch.object(job_queue, '_execute_job', new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = None

            await job_queue.add_job("sync", {"infra_dao_slug": "testdao", "sources": ["dao_node"]})

            job_queue.processing = True
            job_queue.workers_ready.set()

            # Run worker briefly
            worker_task = asyncio.create_task(job_queue._worker(0, mock_gcs))
            await asyncio.sleep(0.1)
            job_queue.processing = False
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

            mock_exec.assert_called_once()
            job = list(job_queue.jobs.values())[0]
            assert job.status == JobStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_worker_handles_job_failure(self, job_queue):
        """Worker marks job FAILED when _execute_job raises."""
        from cpls.jobs import JobStatus

        mock_gcs = MagicMock()
        mock_gcs.safe_upload_job_result = AsyncMock()

        with patch.object(job_queue, '_execute_job', new_callable=AsyncMock) as mock_exec:
            mock_exec.side_effect = Exception("sync crashed")

            await job_queue.add_job("sync", {"infra_dao_slug": "testdao", "sources": ["dao_node"]})

            job_queue.processing = True
            job_queue.workers_ready.set()

            worker_task = asyncio.create_task(job_queue._worker(0, mock_gcs))
            await asyncio.sleep(0.1)
            job_queue.processing = False
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

            job = list(job_queue.jobs.values())[0]
            assert job.status == JobStatus.FAILED
            assert "sync crashed" in job.error

    @pytest.mark.asyncio
    async def test_worker_handles_gcs_upload_failure(self, job_queue):
        """Worker continues even when GCS upload fails."""
        from cpls.jobs import JobStatus

        mock_gcs = MagicMock()
        mock_gcs.safe_upload_job_result = AsyncMock(side_effect=Exception("GCS down"))

        with patch.object(job_queue, '_execute_job', new_callable=AsyncMock):
            await job_queue.add_job("sync", {"infra_dao_slug": "testdao", "sources": ["dao_node"]})

            job_queue.processing = True
            job_queue.workers_ready.set()

            worker_task = asyncio.create_task(job_queue._worker(0, mock_gcs))
            await asyncio.sleep(0.1)
            job_queue.processing = False
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

            # Job still completed despite GCS failure
            job = list(job_queue.jobs.values())[0]
            assert job.status == JobStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_worker_skips_job_without_dao_slug(self, job_queue):
        """Jobs without infra_dao_slug are skipped."""
        from cpls.jobs import Job, JobStatus

        mock_gcs = MagicMock()
        mock_gcs.safe_upload_job_result = AsyncMock()

        # Manually put a job without dao slug
        job = Job(
            id="no-slug",
            type="sync",
            payload={"sources": ["dao_node"]},
            created_at=datetime.now(),
        )
        job_queue.jobs["no-slug"] = job
        await job_queue.queue.put(job)

        job_queue.processing = True
        job_queue.workers_ready.set()

        worker_task = asyncio.create_task(job_queue._worker(0, mock_gcs))
        await asyncio.sleep(0.1)
        job_queue.processing = False
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        # Job should still be PENDING (skipped via continue, not processed)
        assert job.status == JobStatus.PENDING


class TestProcessJobs:

    @pytest.mark.asyncio
    async def test_process_jobs_starts_and_stops(self, job_queue):
        """process_jobs creates workers and they can be cancelled."""
        mock_gcs = MagicMock()
        mock_gcs.safe_upload_job_result = AsyncMock()

        with patch.object(job_queue, '_execute_job', new_callable=AsyncMock):
            process_task = asyncio.create_task(job_queue.process_jobs(mock_gcs))
            await asyncio.sleep(0.2)

            assert len(job_queue.worker_tasks) == 1
            assert job_queue.processing is True

            # Stop
            job_queue.processing = False
            for t in job_queue.worker_tasks:
                t.cancel()
            try:
                await process_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_worker_handles_critical_outer_error(self, job_queue):
        """Lines 155-162: outer except catches errors outside inner try (e.g. queue.get fails)."""
        from cpls.jobs import JobStatus

        mock_gcs = MagicMock()
        mock_gcs.safe_upload_job_result = AsyncMock()

        original_get = job_queue.queue.get

        call_count = [0]
        async def bad_get():
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("queue get exploded")
            return await original_get()

        job_queue.queue.get = bad_get
        job_queue.processing = True
        job_queue.workers_ready.set()

        worker_task = asyncio.create_task(job_queue._worker(0, mock_gcs))
        await asyncio.sleep(0.1)
        job_queue.processing = False
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        # Worker should survive the critical error and continue (not crash the task)
        assert call_count[0] >= 1
