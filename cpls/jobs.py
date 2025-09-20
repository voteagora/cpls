import asyncio
import uuid

from typing import Dict, Optional, List, TYPE_CHECKING
from datetime import datetime
from pydantic import BaseModel
from enum import Enum

if TYPE_CHECKING:
    from gcs import GCSUploader

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

    async def process_jobs(self, gcs_uploader: 'GCSUploader'):
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
                    job.error = str(e)
                finally:
                    job.completed_at = datetime.now()
                    self.current_job = None

                    # Upload result to GCS
                    await gcs_uploader.safe_upload_job_result(job)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error processing job: {e}")

    async def _execute_job(self, job: Job):
        """Execute the actual job logic"""
        # Add your job processing logic here
        print(f"Processing job {job.id} of type {job.type}")

        # Simulate work based on job type
        if job.type == "scheduled":
            await asyncio.sleep(2)  # Simulate scheduled job work
        elif job.type == "external":
            await asyncio.sleep(3)  # Simulate external job work
        else:
            await asyncio.sleep(1)  # Default job work

        print(f"Completed job {job.id}")

    def get_all_jobs(self) -> 'List[Job]':
        """Get all jobs sorted by creation time"""
        return sorted(self.jobs.values(), key=lambda x: x.created_at, reverse=True)

    def stop(self):
        """Stop processing jobs"""
        self.processing = False
