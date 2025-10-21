import asyncio
import uuid
import traceback

from typing import Dict, Optional, List, TYPE_CHECKING
from datetime import datetime
from pydantic import BaseModel
from enum import Enum
from .syncs import DaoNodeSync, EASAtlasSync, EASOoDaoSync

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


class JobRequest(BaseModel):
    type: str
    payload: Dict


class JobQueue:
    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.jobs: Dict[str, Job] = {}
        self.current_job: Optional[Job] = None
        self.processing = False

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

    async def process_jobs(self, gcs_client: 'GCSClient'):
        """Process jobs sequentially from the queue"""
        self.processing = True
        while self.processing:
            try:
                job = await self.queue.get()
                self.current_job = job
                job.status = JobStatus.PROCESSING
                job.started_at = datetime.now()

                try:
                    # Simulate job processing
                    await self._execute_job(job)
                    job.status = JobStatus.COMPLETED
                except Exception as e:
                    job.status = JobStatus.FAILED
                    # Capture the full error message and traceback
                    error_message = str(e)
                    full_traceback = traceback.format_exc()

                    # Store error in job
                    job.error = error_message

                    # Print detailed error information
                    print(f"\n{'='*60}")
                    print(f"❌ JOB FAILED: {job.id}")
                    print(f"Job Type: {job.type}")
                    print(f"Error: {error_message}")
                    print(f"{'='*60}")
                    print("Full Traceback:")
                    print(full_traceback)
                    print(f"{'='*60}\n")
                finally:
                    job.completed_at = datetime.now()
                    self.current_job = None

                    # Upload result to GCS
                    await gcs_client.safe_upload_job_result(job)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"\n{'='*60}")
                print(f"❌ CRITICAL ERROR in job processing loop:")
                print(f"Error: {e}")
                print(f"{'='*60}")
                print("Full Traceback:")
                print(traceback.format_exc())
                print(f"{'='*60}\n")

    async def _execute_job(self, job: Job):
        """Execute the actual job logic"""


        print(job.payload)

        if job.payload['source'] == 'dao_node':
            infra_dao_slug = job.payload['infra_dao_slug']
            gcs_client = GCSClient(GCS_BUCKET_NAME)
            await DaoNodeSync(infra_dao_slug).refresh_list(gcs_client)
        
        elif job.payload['source'] == 'eas-atlas':
            infra_dao_slug = job.payload['infra_dao_slug']
            gcs_client = GCSClient(GCS_BUCKET_NAME)
            await EASAtlasSync(infra_dao_slug).refresh_list(gcs_client)

        elif job.payload['source'] == 'eas-oodao':
            infra_dao_slug = job.payload['infra_dao_slug']
            gcs_client = GCSClient(GCS_BUCKET_NAME)
            await EASOoDaoSync(infra_dao_slug).refresh_list(gcs_client)

        # Add your job processing logic here
        print(f"Processing job {job.id} of type {job.type} for {job.payload['source']}")

        # Simulate work based on job type
        if job.type == "external":
            await asyncio.sleep(10)  # Simulate external job work
        else:
            await asyncio.sleep(10)  # Default job work

        print(f"Completed job {job.id}")

    def get_all_jobs(self) -> 'List[Job]':
        """Get all jobs sorted by creation time"""
        return sorted(self.jobs.values(), key=lambda x: x.created_at, reverse=True)

    def stop(self):
        """Stop processing jobs"""
        self.processing = False
