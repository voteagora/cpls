import asyncio
import uuid
import traceback

from typing import Dict, Optional, List, TYPE_CHECKING
from datetime import datetime
from pydantic import BaseModel
from enum import Enum
import httpx

from .sync_snapshot import SnapshotSync
from .sync_daonode import DaoNodeSync
from .sync_eas_atlas import EASAtlasSync
from .sync_eas_oodao import EASOoDaoSync

import requests as r

from .gcs import GCSClient
from .config import GCS_BUCKET_NAME

class JobStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

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

def create_http_client():
    timeout = httpx.Timeout(15, connect=15)
    limits = httpx.Limits(max_connections=15 * 6, max_keepalive_connections=20)
    return httpx.AsyncClient(timeout=timeout, limits=limits)

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

    async def add_job(self, job_type: str, payload: Dict) -> str:
        job_id = str(uuid.uuid4())
        job = Job(
            id=job_id,
            type=job_type,
            payload=payload,
            created_at=datetime.now()
        )
        self.jobs[job_id] = job
        await self.queue.put(job)
        return job_id

    async def _worker(self, worker_id: int, gcs_client: 'GCSClient'):
        """Worker task that processes jobs from the queue"""
        print(f"🔧 Worker {worker_id} started")

        # Wait for all workers to be ready before consuming
        await self.workers_ready.wait()
        print(f"🔧 Worker {worker_id} ready to process jobs")

        while self.processing:
            try:
                job = await self.queue.get()
                print(f"🔧 Worker {worker_id} got job {job.id}")

                # Extract DAO slug from job payload
                dao_slug = job.payload.get('infra_dao_slug')
                if not dao_slug:
                    print(f"Warning: Job {job.id} has no infra_dao_slug, skipping")
                    self.queue.task_done()
                    continue

                # Acquire lock for this DAO to ensure only one job per DAO
                dao_lock = self._get_dao_lock(dao_slug)

                print(f"🔧 Worker {worker_id} attempting to acquire lock for DAO {dao_slug}")
                try:
                    async with dao_lock:
                        print(f"✅ Worker {worker_id} processing job {job.id} for DAO {dao_slug} (LOCK ACQUIRED)")

                        # Update current_job for backward compatibility (shows last job started)
                        self.current_job = job
                        job.status = JobStatus.PROCESSING
                        job.started_at = datetime.now()

                        try:
                            await self._execute_job(job)
                            job.status = JobStatus.COMPLETED
                        except Exception as e:
                            job.status = JobStatus.FAILED
                            # Capture the full traceback
                            full_traceback = traceback.format_exc()

                            # Store full traceback in job
                            job.error = full_traceback

                            # Print detailed error information
                            print(f"\n{'='*60}")
                            print(f"❌ JOB FAILED: {job.id} (Worker {worker_id})")
                            print(f"Job Type: {job.type}")
                            print(f"DAO: {dao_slug}")
                            print(f"{'='*60}")
                            print("Full Traceback:")
                            print(full_traceback)
                            print(f"{'='*60}\n")
                        finally:
                            job.completed_at = datetime.now()

                            # Upload result to GCS - wrap in try-except to prevent blocking
                            try:
                                await gcs_client.safe_upload_job_result(job)
                            except Exception as upload_error:
                                print(f"⚠️ Failed to upload job result to GCS for job {job.id}: {upload_error}")
                                print(traceback.format_exc())

                            print(f"🔓 Worker {worker_id} finished job {job.id} for DAO {dao_slug} (LOCK RELEASED)")
                finally:
                    # Always call task_done, even if lock acquisition or job processing failed
                    self.queue.task_done()

            except asyncio.CancelledError:
                print(f"Worker {worker_id} cancelled")
                break
            except Exception as e:
                print(f"\n{'='*60}")
                print(f"❌ CRITICAL ERROR in worker {worker_id}:")
                print(f"Error: {e}")
                print(f"{'='*60}")
                print("Full Traceback:")
                print(traceback.format_exc())
                print(f"{'='*60}\n")

        print(f"Worker {worker_id} stopped")

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
        print(f"✅ Started {self.num_workers} concurrent workers")

        # Wait for all workers to complete (when stop() is called)
        try:
            await asyncio.gather(*self.worker_tasks)
        except asyncio.CancelledError:
            print("Job processing cancelled")

    async def _execute_job(self, job: Job):
        """Execute the actual job logic"""

        # Initialize stats structure
        total_skipped = 0
        total_refreshed = 0
        stats_by_source = {}

        gcs_client = GCSClient(GCS_BUCKET_NAME)
        
        for source in job.payload['sources']:

            stats = {
                'skipped': 0,
                'refreshed': 0
            }

            infra_dao_slug = job.payload['infra_dao_slug']
            config = job.payload['config']

            print(f"Job ID: {job}")

            if "refresh_list" == job.payload['logic']:
                reset = job.payload['reset']

                print("Handling {source} for {infra_dao_slug}".format(source=source, infra_dao_slug=infra_dao_slug))
                if source == 'dao_node':
                    stats = await DaoNodeSync(infra_dao_slug, config, reset).refresh_list(gcs_client, self.http_client)
                elif source == 'eas-atlas':
                    stats = await EASAtlasSync(infra_dao_slug, config, reset).refresh_list(gcs_client)
                elif source == 'eas-oodao':
                    stats = await EASOoDaoSync(infra_dao_slug, config, reset).refresh_list(gcs_client)
                elif source == 'snapshot':
                    stats = await SnapshotSync(infra_dao_slug, config, reset).refresh_list(gcs_client)
                else:
                    raise Exception(f"Unknown source: {source}")
                
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

        print(f"Completed job {job.id} - Refreshed: {total_refreshed}, Skipped: {total_skipped}")

    def get_all_jobs(self) -> 'List[Job]':
        """Get all jobs sorted by creation time"""
        return sorted(self.jobs.values(), key=lambda x: x.created_at, reverse=True)

    def stop(self):
        """Stop processing jobs"""
        self.processing = False
        # Cancel all worker tasks
        for task in self.worker_tasks:
            task.cancel()
