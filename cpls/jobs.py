import asyncio
import uuid
import traceback
import time
import json

from typing import Dict, Optional, List
from datetime import datetime
from pydantic import BaseModel
from enum import Enum

from .sync_snapshot import SnapshotSync
from .sync_daonode import DaoNodeSync
from .sync_eas_atlas import EASAtlasSync
from .sync_eas_oodao import EASOoDaoSync

from .gcs import GCSClient
from .config import GCS_BUCKET_NAME, create_http_client
from .observability import emit_event, get_logger

logger = get_logger("cpls.jobs")


def _classify_error(exc: Exception) -> str:
    """Classify job failures by exception type first, then by message substring as fallback."""
    import httpx as _httpx

    if isinstance(exc, _httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, (_httpx.ConnectError, _httpx.ReadError, _httpx.RemoteProtocolError)):
        return "upstream_error"
    if isinstance(exc, (json.JSONDecodeError, ValueError, TypeError, KeyError)):
        return "validation_error"

    s = str(exc).lower()
    if "timeout" in s or "timed out" in s:
        return "timeout"
    if "password authentication" in s or "postgres" in s or "sql" in s:
        return "db_error"
    if "connection" in s or "unreachable" in s:
        return "upstream_error"
    return "unknown"


class JobStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

class Job(BaseModel):
    id: str
    type: str
    payload: Dict
    status: JobStatus = JobStatus.PENDING
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    stats: Optional[Dict] = None


class JobRequest(BaseModel):
    type: str
    payload: Dict

class JobQueue:
    def __init__(self, num_workers: int = 4):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.jobs: Dict[str, Job] = {}
        self.current_job: Optional[Job] = None  # Deprecated, kept for backward compat
        self.processing = False
        self.num_workers = num_workers
        self.dao_locks: Dict[str, asyncio.Lock] = {}
        self.worker_tasks: List[asyncio.Task] = []
        self.workers_ready = asyncio.Event()

        self.http_client = create_http_client()

    def _get_dao_lock(self, dao_slug: str) -> asyncio.Lock:
        """Get or create a lock for a specific DAO"""
        if dao_slug not in self.dao_locks:
            self.dao_locks[dao_slug] = asyncio.Lock()
        return self.dao_locks[dao_slug]

    def _get_oldest_pending_job_age_seconds(self, dao_slug: str) -> float:
        """Age in seconds of the oldest PENDING job for a specific DAO."""
        pending_jobs = [
            job for job in self.jobs.values()
            if job.status == JobStatus.PENDING
            and job.payload.get('infra_dao_slug') == dao_slug
        ]
        if not pending_jobs:
            return 0.0
        oldest_job = min(pending_jobs, key=lambda j: j.created_at)
        return (datetime.now() - oldest_job.created_at).total_seconds()

    def _emit_queue_snapshot(self, infra_dao_slug: str) -> None:
        depth = self.queue.qsize()
        oldest = (
            self._get_oldest_pending_job_age_seconds(infra_dao_slug)
            if infra_dao_slug != "unknown"
            else 0.0
        )
        emit_event(
            "queue.snapshot",
            {
                "infra_dao_slug": infra_dao_slug,
                "depth": depth,
                "oldest_pending_age_seconds": oldest,
            },
        )

    async def add_job(self, job_type: str, payload: Dict) -> str:
        job_id = str(uuid.uuid4())
        job = Job(
            id=job_id,
            type=job_type,
            payload=payload,
            created_at=datetime.now()
        )

        infra_dao_slug = payload.get('infra_dao_slug')
        if not infra_dao_slug:
            raise ValueError(f"Job payload missing required 'infra_dao_slug': {job_id}")
        dao_lock = self._get_dao_lock(infra_dao_slug)
        
        try:
            payload_bytes = len(json.dumps(payload).encode('utf-8'))
        except (TypeError, ValueError):
            payload_bytes = 0
        
        base_fields = {
            "job_id": job_id,
            "infra_dao_slug": infra_dao_slug,
            "job_type": job_type
        }

        if dao_lock.locked():
            job.status = JobStatus.SKIPPED
            job.error = "Queue is full, job skipped"
            self.jobs[job_id] = job

            emit_event("job.skipped", {**base_fields, "reason": "queue_locked"})

            logger.warning("Job skipped due to lock", extra={
                "extra_fields": {
                    "job_id": job_id,
                    "infra_dao_slug": infra_dao_slug,
                    "job_type": job_type,
                    "status": "skipped"
                }
            })
        else:
            self.jobs[job_id] = job
            await self.queue.put(job)

            emit_event("job.queued", {**base_fields, "payload_bytes": payload_bytes})
            self._emit_queue_snapshot(infra_dao_slug)

            logger.info("Job queued", extra={
                "extra_fields": {
                    "job_id": job_id,
                    "infra_dao_slug": infra_dao_slug,
                    "job_type": job_type,
                    "status": "queued"
                }
            })

        return job_id

    async def _worker(self, worker_id: int, gcs_client: 'GCSClient'):
        """Worker task that processes jobs from the queue"""
        logger.info("Worker started", extra={
            "extra_fields": {"worker_id": worker_id}
        })

        # Wait for all workers to be ready before consuming
        await self.workers_ready.wait()
        logger.info("Worker ready to process jobs", extra={
            "extra_fields": {"worker_id": worker_id}
        })

        while self.processing:
            try:
                job = await self.queue.get()
                logger.info("Worker got job", extra={
                    "extra_fields": {
                        "worker_id": worker_id,
                        "job_id": job.id
                    }
                })

                # Extract DAO slug from job payload
                dao_slug = job.payload.get('infra_dao_slug')
                if not dao_slug:
                    logger.warning("Job has no infra_dao_slug, skipping", extra={
                        "extra_fields": {
                            "job_id": job.id,
                            "worker_id": worker_id
                        }
                    })
                    self.queue.task_done()
                    self._emit_queue_snapshot("unknown")
                    continue

                # Acquire lock for this DAO to ensure only one job per DAO
                dao_lock = self._get_dao_lock(dao_slug)

                logger.debug("Worker attempting to acquire lock", extra={
                    "extra_fields": {
                        "worker_id": worker_id,
                        "job_id": job.id,
                        "infra_dao_slug": dao_slug
                    }
                })
                
                try:
                    async with dao_lock:
                        logger.debug("Worker processing job (LOCK ACQUIRED)", extra={
                            "extra_fields": {
                                "worker_id": worker_id,
                                "job_id": job.id,
                                "infra_dao_slug": dao_slug,
                                "job_type": job.type
                            }
                        })

                        # Update current_job for backward compatibility (shows last job started)
                        self.current_job = job
                        job.status = JobStatus.PROCESSING
                        job.started_at = datetime.now()
                        
                        base_fields = {
                            "job_id": job.id,
                            "infra_dao_slug": dao_slug,
                            "job_type": job.type
                        }

                        emit_event("job.started", base_fields)

                        # Track job execution duration
                        start_time = time.time()
                        try:
                            await self._execute_job(job)
                            job.status = JobStatus.COMPLETED

                            duration_ms = (time.time() - start_time) * 1000

                            emit_event(
                                "job.completed",
                                {
                                    **base_fields,
                                    "duration_ms": duration_ms,
                                    "stats": job.stats,
                                },
                            )
                        except Exception as e:
                            job.status = JobStatus.FAILED
                            # Capture the full traceback
                            full_traceback = traceback.format_exc()

                            # Store full traceback in job
                            job.error = full_traceback

                            duration_seconds = time.time() - start_time
                            duration_ms = duration_seconds * 1000

                            error_type = _classify_error(e)
                            failed_fields = {**base_fields, "error_type": error_type}
                            emit_event(
                                "job.failed",
                                {
                                    **failed_fields,
                                    "duration_ms": duration_ms,
                                    "error": str(e),
                                },
                            )

                            logger.error("Job failed", extra={
                                "extra_fields": {
                                    "job_id": job.id,
                                    "infra_dao_slug": dao_slug,
                                    "job_type": job.type,
                                    "status": "failed",
                                    "error_type": error_type,
                                    "duration_seconds": duration_seconds,
                                    "error": str(e),
                                    "traceback": full_traceback
                                }
                            })
                        finally:
                            job.completed_at = datetime.now()

                            # Upload result to GCS - wrap in try-except to prevent blocking
                            try:
                                await gcs_client.safe_upload_job_result(job)
                            except Exception as upload_error:
                                emit_event("gcs.upload_failed", {
                                    **base_fields,
                                    "error": str(upload_error),
                                })
                                logger.error("Failed to upload job result to GCS", extra={
                                    "extra_fields": {
                                        "job_id": job.id,
                                        "error": str(upload_error),
                                        "traceback": traceback.format_exc()
                                    }
                                })

                            logger.info("Worker finished job", extra={
                                "extra_fields": {
                                    "worker_id": worker_id,
                                    "job_id": job.id,
                                    "infra_dao_slug": dao_slug
                                }
                            })
                finally:
                    # Always call task_done, even if lock acquisition or job processing failed
                    self.queue.task_done()
                    self._emit_queue_snapshot(dao_slug)

            except asyncio.CancelledError:
                logger.debug("Worker cancelled", extra={
                    "extra_fields": {"worker_id": worker_id}
                })
                break
            except Exception as e:
                logger.error("Critical error in worker", extra={
                    "extra_fields": {
                        "worker_id": worker_id,
                        "error": str(e),
                        "traceback": traceback.format_exc()
                    }
                })

        logger.debug("Worker stopped", extra={
            "extra_fields": {"worker_id": worker_id}
        })

    async def process_jobs(self, gcs_client: 'GCSClient'):
        """Start concurrent workers to process jobs from the queue"""
        self.processing = True

        # Spawn worker tasks
        for i in range(self.num_workers):
            task = asyncio.create_task(self._worker(i, gcs_client))
            self.worker_tasks.append(task)

        # Give all workers a chance to start up
        await asyncio.sleep(0.1)

        # Signal all workers to start consuming
        self.workers_ready.set()
        logger.debug("Started concurrent workers", extra={
            "extra_fields": {"num_workers": self.num_workers}
        })

        # Wait for all workers to complete (when stop() is called)
        try:
            await asyncio.gather(*self.worker_tasks)
        except asyncio.CancelledError:
            logger.debug("Job processing cancelled")

    async def _execute_job(self, job: Job):
        """Execute the actual job logic"""

        # Initialize stats structure
        total_skipped = 0
        total_refreshed = 0
        stats_by_source = {}

        gcs_client = GCSClient(GCS_BUCKET_NAME)
        infra_dao_slug = job.payload['infra_dao_slug']

        for source in job.payload['sources']:

            stats = {
                'skipped': 0,
                'refreshed': 0
            }

            config = job.payload['config']

            if "refresh_list" == job.payload['logic']:
                reset = job.payload['reset']

                logger.debug("Processing source", extra={
                    "extra_fields": {
                        "job_id": job.id,
                        "infra_dao_slug": infra_dao_slug,
                        "source": source
                    }
                })
                
                # Track refresh duration
                refresh_start = time.time()
                try:
                    if source == 'dao_node':
                        stats = await DaoNodeSync(infra_dao_slug, config, reset, self.http_client).refresh_list(gcs_client)
                    elif source == 'eas-atlas':
                        stats = await EASAtlasSync(infra_dao_slug, config, reset, self.http_client).refresh_list(gcs_client)
                    elif source == 'eas-oodao':
                        stats = await EASOoDaoSync(infra_dao_slug, config, reset, self.http_client).refresh_list(gcs_client)
                    elif source == 'snapshot':
                        stats = await SnapshotSync(infra_dao_slug, config, reset, self.http_client).refresh_list(gcs_client)
                    else:
                        raise Exception(f"Unknown source: {source}")
                finally:
                    refresh_duration_ms = (time.time() - refresh_start) * 1000
                    stats["duration_ms"] = round(refresh_duration_ms, 3)

            # Collect stats
            if stats:
                stats_by_source[source] = stats
                total_skipped += stats.get('skipped', 0)
                total_refreshed += stats.get('refreshed', 0)

        # Store stats in the job
        job.stats = {
            'total_skipped': total_skipped,
            'total_refreshed': total_refreshed,
            'by_source': stats_by_source
        }

    def get_all_jobs(self) -> 'List[Job]':
        """Get all jobs sorted by creation time"""
        return sorted(self.jobs.values(), key=lambda x: x.created_at, reverse=True)

    def stop(self):
        """Stop processing jobs"""
        self.processing = False
        # Cancel all worker tasks
        for task in self.worker_tasks:
            task.cancel()
